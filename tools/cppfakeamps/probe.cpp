#include <amps/ampsplusplus.hpp>
#include <amps/MemoryPublishStore.hpp>

#include <chrono>
#include <condition_variable>
#include <exception>
#include <iostream>
#include <mutex>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

using Delivery = std::pair<std::string, std::string>;

struct Deliveries {
  std::mutex mutex;
  std::condition_variable changed;
  std::vector<Delivery> messages;
  std::vector<std::string> bookmarks;
  std::vector<std::string> leases;
};

struct OOFEvents {
  std::mutex mutex;
  std::condition_variable changed;
  std::vector<std::string> sowPayloads;
  std::vector<std::tuple<std::string, std::string, std::string, std::string>> oof;
  int groupsEnded = 0;
};

void receive(const AMPS::Message& message, void* opaque) {
  if (message.getCommandEnum() != AMPS::Message::Command::Publish) {
    return;
  }
  auto& deliveries = *static_cast<Deliveries*>(opaque);
  std::lock_guard<std::mutex> lock(deliveries.mutex);
  deliveries.messages.emplace_back(static_cast<std::string>(message.getTopic()),
                                  static_cast<std::string>(message.getData()));
  deliveries.bookmarks.push_back(static_cast<std::string>(message.getBookmark()));
  deliveries.leases.push_back(static_cast<std::string>(message.getLeasePeriod()));
  deliveries.changed.notify_all();
}

void receiveOOF(const AMPS::Message& message, void* opaque) {
  auto& events = *static_cast<OOFEvents*>(opaque);
  std::lock_guard<std::mutex> lock(events.mutex);
  if (message.getCommandEnum() == AMPS::Message::Command::SOW) {
    events.sowPayloads.push_back(static_cast<std::string>(message.getData()));
  } else if (message.getCommandEnum() == AMPS::Message::Command::GroupEnd) {
    ++events.groupsEnded;
  } else if (message.getCommandEnum() == AMPS::Message::Command::OOF) {
    events.oof.emplace_back(static_cast<std::string>(message.getTopic()),
                            static_cast<std::string>(message.getSowKey()),
                            static_cast<std::string>(message.getReason()),
                            static_cast<std::string>(message.getData()));
  }
  events.changed.notify_all();
}

void expectDeliveries(Deliveries& deliveries, const std::vector<Delivery>& expected,
                      int quietMilliseconds = 250) {
  std::unique_lock<std::mutex> lock(deliveries.mutex);
  if (!expected.empty()) {
    deliveries.changed.wait_for(lock, std::chrono::seconds(2), [&] {
      return deliveries.messages.size() >= expected.size();
    });
  }
  deliveries.changed.wait_for(lock, std::chrono::milliseconds(quietMilliseconds), [&] {
    return deliveries.messages.size() > expected.size();
  });
  if (deliveries.messages != expected) {
    std::ostringstream detail;
    detail << "expected " << expected.size() << " deliveries";
    for (const auto& item : expected) {
      detail << " [" << item.first << " " << item.second << "]";
    }
    detail << ", received " << deliveries.messages.size();
    for (const auto& item : deliveries.messages) {
      detail << " [" << item.first << " " << item.second << "]";
    }
    throw std::runtime_error(detail.str());
  }
}

std::string queueBookmark(Deliveries& deliveries, std::size_t index) {
  std::lock_guard<std::mutex> lock(deliveries.mutex);
  if (index >= deliveries.bookmarks.size() || deliveries.bookmarks[index].empty()) {
    throw std::runtime_error("queue delivery has no bookmark");
  }
  if (deliveries.leases[index] != "1500ms") {
    throw std::runtime_error("unexpected queue lease: " + deliveries.leases[index]);
  }
  return deliveries.bookmarks[index];
}

void acknowledge(AMPS::Client& subscriber, const std::string& topic,
                 const std::string& bookmark) {
  subscriber.ack(topic, bookmark);
  subscriber.flushAcks();
  subscriber.publishFlush(2000);
}

void connect(AMPS::Client& client, const char* uri) {
  client.connect(uri);
  client.logon(2000);
}

void live(const char* uri) {
  Deliveries deliveries;
  AMPS::Client subscriber("cppfakeamps-live-subscriber");
  AMPS::Client publisher("cppfakeamps-live-publisher");
  connect(subscriber, uri);
  connect(publisher, uri);
  const std::string topic = "cpp-live-\"quoted\"-\\backslash";
  const std::string payload = "{\"id\":1,\"value\":\"cpp\"}";
  subscriber.subscribe(AMPS::MessageHandler(receive, &deliveries), topic, 2000);
  publisher.publish(topic, payload);
  publisher.publishFlush(2000);
  expectDeliveries(deliveries, {{topic, payload}});
}

void bookmarkEpoch(const char* uri) {
  Deliveries deliveries;
  AMPS::Client subscriber("cppfakeamps-epoch-subscriber");
  AMPS::Client publisher("cppfakeamps-epoch-publisher");
  connect(subscriber, uri);
  connect(publisher, uri);
  const std::string topic = "cpp-epoch";
  const std::string historical1 = "{\"id\":1,\"value\":\"historical\"}";
  const std::string historical2 = "{\"id\":2,\"value\":\"historical\"}";
  const std::string livePayload = "{\"id\":3,\"value\":\"live\"}";
  publisher.publish(topic, historical1);
  publisher.publish(topic, historical2);
  publisher.publishFlush(2000);
  subscriber.bookmarkSubscribe(AMPS::MessageHandler(receive, &deliveries), topic,
                               2000, AMPS::Client::BOOKMARK_EPOCH());
  expectDeliveries(deliveries, {{topic, historical1}, {topic, historical2}});
  publisher.publish(topic, livePayload);
  publisher.publishFlush(2000);
  expectDeliveries(deliveries, {{topic, historical1}, {topic, historical2},
                                {topic, livePayload}});
}

void bookmarkNow(const char* uri) {
  Deliveries deliveries;
  AMPS::Client subscriber("cppfakeamps-now-subscriber");
  AMPS::Client publisher("cppfakeamps-now-publisher");
  connect(subscriber, uri);
  connect(publisher, uri);
  const std::string topic = "cpp-now";
  const std::string historical = "{\"id\":3,\"value\":\"historical\"}";
  const std::string livePayload = "{\"id\":4,\"value\":\"live\"}";
  publisher.publish(topic, historical);
  publisher.publishFlush(2000);
  subscriber.bookmarkSubscribe(AMPS::MessageHandler(receive, &deliveries), topic,
                               2000, AMPS::Client::NOW());
  expectDeliveries(deliveries, {});
  publisher.publish(topic, livePayload);
  publisher.publishFlush(2000);
  expectDeliveries(deliveries, {{topic, livePayload}});
}

void bookmarkResume(const char* uri) {
  Deliveries witness;
  Deliveries resumed;
  AMPS::Client witnessClient("cppfakeamps-resume-witness");
  AMPS::Client subscriber("cppfakeamps-resume-subscriber");
  AMPS::Client publisher("cppfakeamps-resume-publisher");
  connect(witnessClient, uri);
  connect(subscriber, uri);
  connect(publisher, uri);
  const std::string topic = "cpp-resume";
  const std::string first = "{\"id\":1,\"value\":\"first\"}";
  const std::string second = "{\"id\":2,\"value\":\"second\"}";
  const std::string third = "{\"id\":3,\"value\":\"live\"}";
  witnessClient.subscribe(AMPS::MessageHandler(receive, &witness), topic, 2000);
  publisher.publish(topic, first);
  publisher.publishFlush(2000);
  expectDeliveries(witness, {{topic, first}});
  std::string bookmark;
  {
    std::lock_guard<std::mutex> lock(witness.mutex);
    bookmark = witness.bookmarks[0];
  }
  if (bookmark.empty()) {
    throw std::runtime_error("first delivery has no bookmark");
  }
  witnessClient.disconnect();
  publisher.publish(topic, second);
  publisher.publishFlush(2000);
  subscriber.bookmarkSubscribe(AMPS::MessageHandler(receive, &resumed), topic,
                               2000, bookmark);
  expectDeliveries(resumed, {{topic, second}});
  publisher.publish(topic, third);
  publisher.publishFlush(2000);
  expectDeliveries(resumed, {{topic, second}, {topic, third}});
}

void sow(const char* uri) {
  AMPS::Client publisher("cppfakeamps-sow-publisher");
  AMPS::Client reader("cppfakeamps-sow-reader");
  connect(publisher, uri);
  connect(reader, uri);
  const std::string topic = "cpp-sow";
  const std::multiset<std::string> expected = {
      "{\"id\":1}", "{\"id\":2}", "{\"id\":3}"};
  publisher.publishKeyed(topic, "{\"id\":1}", "key1");
  publisher.publishKeyed(topic, "{\"id\":2}", "key2");
  publisher.publishKeyed(topic, "{\"id\":3}", "key3");
  publisher.publishFlush(2000);

  auto stream = reader.sow(topic, "", "", "", 2, -1, "", 2000);
  stream.timeout(2000);
  std::multiset<std::string> actual;
  int begin = 0;
  int end = 0;
  for (auto it = stream.begin(); it != stream.end(); ++it) {
    if (!it->isValid()) {
      throw std::runtime_error("timed out waiting for SOW completion");
    }
    switch (it->getCommandEnum()) {
      case AMPS::Message::Command::GroupBegin:
        ++begin;
        break;
      case AMPS::Message::Command::GroupEnd:
        ++end;
        break;
      case AMPS::Message::Command::SOW:
        actual.insert(static_cast<std::string>(it->getData()));
        break;
      default:
        throw std::runtime_error("unexpected SOW stream command");
    }
  }
  if (begin != 1 || end != 1 || actual != expected) {
    std::ostringstream detail;
    detail << "expected one group and three records; got begin=" << begin
           << " end=" << end << " records=" << actual.size();
    for (const auto& item : actual) {
      detail << " [" << item << "]";
    }
    throw std::runtime_error(detail.str());
  }
}

void queueAck(const char* uri) {
  Deliveries deliveries;
  AMPS::Client subscriber("cppfakeamps-queue-subscriber");
  AMPS::Client publisher("cppfakeamps-queue-publisher");
  connect(subscriber, uri);
  connect(publisher, uri);
  subscriber.setAutoAck(false);
  subscriber.setAckBatchSize(1);
  const std::string topic = "queue://cpp-ack";
  const std::string first = "{\"id\":1}";
  const std::string second = "{\"id\":2}";
  subscriber.subscribe(AMPS::MessageHandler(receive, &deliveries), topic,
                       2000, "", "max_backlog=1");
  publisher.publishKeyed(topic, first, "queue-key1");
  publisher.publishFlush(2000);
  expectDeliveries(deliveries, {{topic, first}});
  publisher.publishKeyed(topic, second, "queue-key2");
  publisher.publishFlush(2000);
  expectDeliveries(deliveries, {{topic, first}});
  acknowledge(subscriber, topic, queueBookmark(deliveries, 0));
  expectDeliveries(deliveries, {{topic, first}, {topic, second}});
  acknowledge(subscriber, topic, queueBookmark(deliveries, 1));
  expectDeliveries(deliveries, {{topic, first}, {topic, second}}, 1800);
}

void queueLease(const char* uri) {
  Deliveries firstDelivery;
  Deliveries redelivery;
  AMPS::Client firstSubscriber("cppfakeamps-lease-first");
  AMPS::Client secondSubscriber("cppfakeamps-lease-second");
  AMPS::Client publisher("cppfakeamps-lease-publisher");
  connect(firstSubscriber, uri);
  connect(secondSubscriber, uri);
  connect(publisher, uri);
  firstSubscriber.setAutoAck(false);
  secondSubscriber.setAutoAck(false);
  secondSubscriber.setAckBatchSize(1);
  const std::string topic = "queue://cpp-lease";
  const std::string payload = "{\"id\":3}";
  firstSubscriber.subscribe(AMPS::MessageHandler(receive, &firstDelivery),
                            topic, 2000, "", "", "first");
  publisher.publishKeyed(topic, payload, "queue-key3");
  publisher.publishFlush(2000);
  expectDeliveries(firstDelivery, {{topic, payload}});
  const std::string firstBookmark = queueBookmark(firstDelivery, 0);
  secondSubscriber.subscribe(AMPS::MessageHandler(receive, &redelivery),
                             topic, 2000, "", "", "second");
  expectDeliveries(redelivery, {{topic, payload}});
  const std::string redeliveredBookmark = queueBookmark(redelivery, 0);
  if (firstBookmark != redeliveredBookmark) {
    throw std::runtime_error("redelivery changed the queue bookmark");
  }
  acknowledge(secondSubscriber, topic, redeliveredBookmark);
  expectDeliveries(redelivery, {{topic, payload}}, 1800);
  expectDeliveries(firstDelivery, {{topic, payload}});
}

void queueSingleLease(const char* uri) {
  Deliveries deliveries;
  AMPS::Client subscriber("cppfakeamps-single-lease-subscriber");
  AMPS::Client publisher("cppfakeamps-single-lease-publisher");
  connect(subscriber, uri);
  connect(publisher, uri);
  subscriber.setAutoAck(false);
  subscriber.setAckBatchSize(1);
  const std::string topic = "queue://cpp-single-lease";
  const std::string payload = "{\"id\":4}";
  subscriber.subscribe(AMPS::MessageHandler(receive, &deliveries), topic, 2000);
  publisher.publishKeyed(topic, payload, "queue-key4");
  publisher.publishFlush(2000);
  expectDeliveries(deliveries, {{topic, payload}});
  const std::string bookmark = queueBookmark(deliveries, 0);
  expectDeliveries(deliveries, {{topic, payload}, {topic, payload}});
  if (bookmark != queueBookmark(deliveries, 1)) {
    throw std::runtime_error("redelivery changed the queue bookmark");
  }
  acknowledge(subscriber, topic, bookmark);
  expectDeliveries(deliveries, {{topic, payload}, {topic, payload}}, 1800);
}

void retainedReconnect(const char* uri) {
  Deliveries deliveries;
  AMPS::Client subscriber("cppfakeamps-retained-subscriber");
  AMPS::Client publisher("cppfakeamps-retained-publisher");
  AMPS::Store store(new AMPS::MemoryPublishStore(10));
  publisher.setPublishStore(store);
  connect(subscriber, uri);
  connect(publisher, uri);
  const std::string topic = "cpp-retained";
  const std::string first = "{\"id\":1,\"value\":\"before\"}";
  const std::string second = "{\"id\":2,\"value\":\"offline\"}";
  subscriber.subscribe(AMPS::MessageHandler(receive, &deliveries), topic, 2000);
  const auto firstSequence = publisher.publish(topic, first);
  publisher.publishFlush(2000);
  expectDeliveries(deliveries, {{topic, first}});
  if (firstSequence == 0 || store.unpersistedCount() != 0 ||
      store.getLastPersisted() != firstSequence) {
    throw std::runtime_error("initial publish was not confirmed persisted");
  }
  publisher.disconnect();
  try {
    publisher.publish(topic, second);
  } catch (const AMPS::DisconnectedException&) {
    // The store retains this publish even when sending reports disconnection.
  }
  if (store.unpersistedCount() != 1) {
    throw std::runtime_error("offline publish was not retained exactly once");
  }
  expectDeliveries(deliveries, {{topic, first}});
  connect(publisher, uri);
  publisher.publishFlush(2000);
  expectDeliveries(deliveries, {{topic, first}, {topic, second}});
  if (store.unpersistedCount() != 0 || store.getLastPersisted() <= firstSequence) {
    throw std::runtime_error("replayed publish was not confirmed persisted");
  }
  publisher.disconnect();
  connect(publisher, uri);
  expectDeliveries(deliveries, {{topic, first}, {topic, second}});
}

void oof(const char* uri) {
  OOFEvents events;
  AMPS::Client subscriber("cppfakeamps-oof-subscriber");
  AMPS::Client publisher("cppfakeamps-oof-publisher");
  connect(subscriber, uri);
  connect(publisher, uri);
  const std::string topic = "cpp-oof";
  const std::string payload = "{\"id\":1}";
  publisher.publishKeyed(topic, payload, "oof-key");
  publisher.publishFlush(2000);
  subscriber.sowAndSubscribe(AMPS::MessageHandler(receiveOOF, &events), topic,
                             2000, "", 2, true);
  {
    std::unique_lock<std::mutex> lock(events.mutex);
    events.changed.wait_for(lock, std::chrono::seconds(2), [&] {
      return events.groupsEnded > 0;
    });
    if (events.groupsEnded != 1 || events.sowPayloads != std::vector<std::string>{payload}) {
      throw std::runtime_error("initial SOW did not contain exactly one expected record and group end");
    }
  }
  const AMPS::Message deleted = publisher.sowDelete(topic, "/id = 1", 2000);
  if (!deleted.isValid() || static_cast<std::string>(deleted.getMatches()) != "1") {
    throw std::runtime_error("SOW delete did not report one deleted record");
  }
  {
    std::unique_lock<std::mutex> lock(events.mutex);
    events.changed.wait_for(lock, std::chrono::seconds(2), [&] {
      return !events.oof.empty();
    });
    events.changed.wait_for(lock, std::chrono::milliseconds(250), [&] {
      return events.oof.size() > 1;
    });
    const std::vector<std::tuple<std::string, std::string, std::string, std::string>> expected = {
        {topic, "oof-key", "delete", ""}};
    if (events.oof != expected) {
      std::ostringstream detail;
      detail << "expected one delete OOF for " << topic << " key oof-key, got "
             << events.oof.size();
      for (const auto& event : events.oof) {
        detail << " [" << std::get<0>(event) << " " << std::get<1>(event)
               << " " << std::get<2>(event) << " " << std::get<3>(event) << "]";
      }
      throw std::runtime_error(detail.str());
    }
  }
}

int main(int argc, char** argv) {
  if (argc != 3) {
    std::cerr << "usage: cppfakeamps <uri> <scenario>\n";
    return 2;
  }
  const std::string scenario = argv[2];
  try {
    if (scenario == "live") {
      live(argv[1]);
    } else if (scenario == "bookmark-epoch") {
      bookmarkEpoch(argv[1]);
    } else if (scenario == "bookmark-now") {
      bookmarkNow(argv[1]);
    } else if (scenario == "bookmark-resume") {
      bookmarkResume(argv[1]);
    } else if (scenario == "sow") {
      sow(argv[1]);
    } else if (scenario == "queue-ack") {
      queueAck(argv[1]);
    } else if (scenario == "queue-lease") {
      queueLease(argv[1]);
    } else if (scenario == "queue-single-lease") {
      queueSingleLease(argv[1]);
    } else if (scenario == "retained-reconnect") {
      retainedReconnect(argv[1]);
    } else if (scenario == "oof") {
      oof(argv[1]);
    } else {
      std::cerr << "unknown scenario: " << scenario << '\n';
      return 2;
    }
    std::cout << "PASS " << scenario << '\n';
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "FAIL " << scenario << ": " << error.what() << '\n';
    return 1;
  }
}
