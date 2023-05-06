package org.improving.workshop.project
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.apache.kafka.streams.TopologyTestDriver
import org.improving.workshop.Streams
import org.improving.workshop.samples.PurchaseEventTicket
import org.msse.demo.mockdata.customer.address.Address
import org.msse.demo.mockdata.customer.profile.Customer
import org.msse.demo.mockdata.music.event.Event
import org.msse.demo.mockdata.music.ticket.Ticket
import org.msse.demo.mockdata.music.venue.Venue
import spock.lang.Specification
import static org.improving.workshop.utils.DataFaker.ARTISTS
import static org.improving.workshop.utils.DataFaker.STREAMS
import static org.improving.workshop.utils.DataFaker.STREAMS
import static org.improving.workshop.utils.DataFaker.STREAMS
import static org.improving.workshop.utils.DataFaker.STREAMS
import static org.improving.workshop.utils.DataFaker.STREAMS
class SuperfansSpec extends Specification {
  TopologyTestDriver driver
  // inputs
  TestInputTopic<String, Event> eventInputTopic
  TestInputTopic<String, Ticket> ticketInputTopic
  TestInputTopic<String, Venue> venueInputTopic
  TestInputTopic<String, Address> addressInputTopic
  // outputs
  TestOutputTopic<String, Superfans.Superfan> outputTopic

  def 'setup'() {
    // instantiate new builder
    StreamsBuilder streamsBuilder = new StreamsBuilder()

    // build the RemainingEventTickets topology (by reference)
    Superfans.configureTopology(streamsBuilder)

    // build the TopologyTestDriver
    driver = new TopologyTestDriver(streamsBuilder.build(), Streams.buildProperties())

    eventInputTopic = driver.createInputTopic(
      Streams.TOPIC_DATA_DEMO_EVENTS,
      Serdes.String().serializer(),
      Streams.SERDE_EVENT_JSON.serializer()
    )

    venueInputTopic = driver.createInputTopic(
      Streams.TOPIC_DATA_DEMO_VENUES,
      Serdes.String().serializer(),
      Streams.SERDE_VENUE_JSON.serializer()
    )

    addressInputTopic = driver.createInputTopic(
      Streams.TOPIC_DATA_DEMO_ADDRESSES,
      Serdes.String().serializer(),
      Streams.SERDE_ADDRESS_JSON.serializer()
    )

    ticketInputTopic = driver.createInputTopic(
      Streams.TOPIC_DATA_DEMO_TICKETS,
      Serdes.String().serializer(),
      Streams.SERDE_TICKET_JSON.serializer()
    )

    outputTopic = driver.createOutputTopic(
      Superfans.SUPERFAN_TOPIC,
      Serdes.String().deserializer(),
      Superfans.SUPERFAN_JSON_SERDE.deserializer()
    )
  }

  def 'cleanup'() {
    // if this close doesn't run (test fails halfway through), subsequent tests may start on old state
    // run the test and let it cleanup, then run the test again.
    driver.close()
  }

  def 'it processes super fans and returns the correct events'() {
    given: "Addresses for venues in 2 states"
    addressInputTopic.pipeInput("mn-address", new Address("mn-address", "", "US", "V", "123 4th St", "", "Minneapolis", "MN", "55414", "1234", "US"))
    addressInputTopic.pipeInput("ca-address", new Address("ca-address", "", "US", "V", "567 8th St", "", "Los Angeles", "CA", "90005", "1234", "US"))

    and: "venues with those addresses"
    venueInputTopic.pipeInput("mn-venue", new Venue("mn-venue", "mn-address", "MN Music", 500))
    venueInputTopic.pipeInput("ca-venue", new Venue("ca-venue", "ca-address", "CA Events", 5000))

    and: "events at those venues"
    def artistId = "the-best-artist"
    eventInputTopic.pipeInput("mn-show", new Event("mn-show", artistId, "mn-venue", 500, "today"))
    eventInputTopic.pipeInput("ca-show", new Event("ca-show", artistId, "ca-venue", 1000, "tomorrow"))

    and: "a customer that purchases tickets for both of those events"
    def superFanId = "super-fan-id"
    ticketInputTopic.pipeInput("ticket-1", new Ticket("ticket-1", superFanId, "mn-show", 200))
    ticketInputTopic.pipeInput("ticket-2", new Ticket("ticket-2", superFanId, "ca-show", 250))

    when: "outputs are received"
    def outputRecords = outputTopic.readRecordsToList()

    then: "there is one event"
    outputRecords.size() == 1

    and: "the event has the correct fan"
    outputRecords.first().value().customerID == superFanId

    and: "the event has the correct artist"
    outputRecords.first().value().artistID == artistId

    and: "the list of states is correct"
    outputRecords.first().value().stateList == ["MN", "CA"]
  }
}