package org.improving.workshop.project;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.improving.workshop.Streams;
import org.msse.demo.mockdata.music.artist.Artist;
import org.msse.demo.mockdata.music.event.Event;
import org.msse.demo.mockdata.music.ticket.Ticket;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.io.Serializable;
import java.util.*;

import static java.util.Collections.reverseOrder;
import static java.util.Collections.sort;
import static java.util.stream.Collectors.toMap;
import static org.apache.kafka.streams.state.Stores.persistentKeyValueStore;
import static org.improving.workshop.Streams.*;

/**
 * Class for solution to Question 1
 */
@Slf4j
public class ArtistTicketRatio {
    public static final String OUTPUT_TOPIC = "artist-ticket-ratio";
    public static final JsonSerde<StreamsPerArtist2> STREAMS_PER_ARTIST2_JSON_SERDE = new JsonSerde<>(StreamsPerArtist2.class);
    public static final JsonSerde<TicketsPerArtist2> TICKETS_PER_ARTIST2_JSON_SERDE = new JsonSerde<>(TicketsPerArtist2.class);
    public static final JsonSerde<ArtistRatio> ARTIST_RATIO_JSON_SERDE = new JsonSerde<>(ArtistRatio.class);
    public static final JsonSerde<ArtistMetrics> ARTIST_METRICS_JSON_SERDE = new JsonSerde<>(ArtistMetrics.class);
    public static final JsonSerde<EventTicket> EVENT_TICKET_JSON_SERDE = new JsonSerde<>(EventTicket.class);
    public static final JsonSerde<ArtistNameRatio> ARTIST_NAME_RATIO_JSON_SERDE = new JsonSerde<>(ArtistNameRatio.class);
    public static final JsonSerde<ArtistTopRatio> ARTIST_TOP_RATIO_JSON_SERDE = new JsonSerde<>(ArtistTopRatio.class);


    static KTable<String, Artist> getArtistTable(final StreamsBuilder builder) {
        return builder
                .table(
                        TOPIC_DATA_DEMO_ARTISTS,
                        Materialized
                                .<String, Artist>as(persistentKeyValueStore("artists"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Streams.SERDE_ARTIST_JSON)
                );

    }
    static KTable<String, TicketsPerArtist2> getArtistTicketTable(final StreamsBuilder builder) {
        KTable<String, Event> eventsTable = builder
                .table(
                        TOPIC_DATA_DEMO_EVENTS,
                        Materialized
                                .<String, Event>as(persistentKeyValueStore("events"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Streams.SERDE_EVENT_JSON)
                );

        return builder
                .stream(TOPIC_DATA_DEMO_TICKETS, Consumed.with(Serdes.String(), SERDE_TICKET_JSON))
                .selectKey((ticketId, ticket) -> ticket.eventid(), Named.as("rekey-by-eventid"))
                .join(
                        eventsTable,
                        (eventId, ticket, event) -> new EventTicket(ticket, event)
                )
                .groupBy((k, v) -> v.event.artistid(), Grouped.with(Serdes.String(), EVENT_TICKET_JSON_SERDE))
                .aggregate(
                        TicketsPerArtist2::new,
                        (artistId, eventTicket, artistTicketsCounts) -> {
                            artistTicketsCounts.setArtistId(artistId);
                            artistTicketsCounts.addTicketIncrement();
                            return artistTicketsCounts;
                        },
                        Materialized
                                .<String, TicketsPerArtist2>as(persistentKeyValueStore("tickets-per-artist-counts"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(TICKETS_PER_ARTIST2_JSON_SERDE)
                );
    }

    static void configureTopology(final StreamsBuilder builder) {
        KTable<String, TicketsPerArtist2> ticketsPerArtistTable = getArtistTicketTable(builder);

        KTable<String, Artist> artistsTable = getArtistTable(builder);

        KTable<String, StreamsPerArtist2> streamsPerArtistTable = builder.stream(TOPIC_DATA_DEMO_STREAMS, Consumed.with(Serdes.String(), SERDE_STREAM_JSON))
                .groupBy((k, v) -> v.artistid(), Grouped.with(Serdes.String(), SERDE_STREAM_JSON))
                .aggregate(
                        StreamsPerArtist2::new,
                        (artistId, stream, artistStreamCounts) -> {
                            artistStreamCounts.setArtistId(artistId);
                            artistStreamCounts.addStreamIncrement();
                            return artistStreamCounts;
                        },

                        Materialized
                                .<String, StreamsPerArtist2>as(persistentKeyValueStore("stream-per-artist-counts"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(STREAMS_PER_ARTIST2_JSON_SERDE)
                );
        KTable<String, ArtistRatio> artistRatioTable = streamsPerArtistTable.toStream()
                .join(ticketsPerArtistTable, (artistId, ticketsPerArtist, streamsPerArtist) -> new ArtistMetrics(streamsPerArtist, ticketsPerArtist))
                .groupByKey(Grouped.with(Serdes.String(), ARTIST_METRICS_JSON_SERDE))
                .aggregate(
                        ArtistRatio::new,
                        (artistId, artistMetrics, artistRatioCounter) -> {
                            artistRatioCounter.setRatio(artistMetrics.ticketsPerArtist.getCount(), artistMetrics.streamsPerArtist.getCount(), artistId);
                            return artistRatioCounter;
                        },
                        Materialized
                                .<String, ArtistRatio>as(persistentKeyValueStore("artist-ratio"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(ARTIST_RATIO_JSON_SERDE));

        artistRatioTable
                .toStream()
                .join(artistsTable, (artistId, artistRatio, artist) -> new ArtistNameRatio(artistRatio.ratio, artist.id(), artist.name()))
                .selectKey((artistId, artistRatio) -> "X")
                .groupByKey(Grouped.with(Serdes.String(), ARTIST_NAME_RATIO_JSON_SERDE))
                .aggregate(
                        ArtistTopRatio::new,
                        (artistId, artistRatio, ratioCounter) -> {
                            ratioCounter.determinePlacement(artistRatio);
                            return ratioCounter;
                        },
                        Materialized
                                .<String, ArtistTopRatio>as(persistentKeyValueStore("artistTopRatio"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(ARTIST_TOP_RATIO_JSON_SERDE))
                .toStream()
                //.mapValues(artistTop5Ratio -> artistTop5Ratio.top(3))
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), ARTIST_TOP_RATIO_JSON_SERDE));
    }

    public static <K, V extends Comparable<? super V>> Comparator<Map.Entry<K, ArtistNameRatio>> comparingByValue() {
        return (Comparator<Map.Entry<K, ArtistNameRatio>> & Serializable)
                (c1, c2) -> Double.compare(c1.getValue().artistRatio, c2.getValue().artistRatio);
    }

    public static void main(final String[] args) {
        final StreamsBuilder builder = new StreamsBuilder();

        // configure the processing topology
        configureTopology(builder);

        // fire up the engines
        startStreams(builder);
    }

    @Data
    @Getter
    @Setter
    @AllArgsConstructor
    public static class ArtistNameRatio {
        private double artistRatio;
        private String artistId;
        private String name;
        public ArtistNameRatio() {
        }
    }

    @Data
    @AllArgsConstructor
    public static class ArtistRatio {
        private double ratio;
        private String artistId;
        public ArtistRatio() {
        }
        public void setRatio(int ticketCount, int streamCount, String id) {
            ratio = (double) ticketCount / streamCount;
            artistId = id;
        }
    }

    @Data
    @AllArgsConstructor
    public static class ArtistTopRatio {
        private LinkedHashMap<String, ArtistNameRatio> map;
        public ArtistTopRatio() {
            this.map = new LinkedHashMap<>();
        }

        public void determinePlacement(ArtistNameRatio newArtistRatio) {
           map.put(newArtistRatio.artistId, newArtistRatio);
            this.map = map.entrySet().stream()
                    .sorted(reverseOrder(comparingByValue()))
                    .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
        }
    }

    @Data
    @AllArgsConstructor
    public static class ArtistMetrics {
        private TicketsPerArtist2 ticketsPerArtist;
        private StreamsPerArtist2 streamsPerArtist;
    }

    @Data
    @AllArgsConstructor
    public static class EventTicket {
        private Ticket ticket;
        private Event event;
        public EventTicket() {}
    }

    @Data
    @AllArgsConstructor
    public static class StreamsPerArtist2 {
        private String artistId;
        private int count;

        public StreamsPerArtist2() {

        }

        public void addStreamIncrement() {
            count++;
        }
    }

    @Data
    @AllArgsConstructor
    public static class TicketsPerArtist2 {
        private String artistId;
        private int count;

        public TicketsPerArtist2() {

        }

        public void addTicketIncrement() {
            count++;
        }
    }
}
