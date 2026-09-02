Feature: Distributed queues
  A distributed queue places each of its slots on its own machine, so it spreads
  over the cluster and losing one node costs only that node's slots.

  Scenario: Slots spread across the cluster
    When I create a distributed queue
    Then its slots are spread over more than one node
    And every slot belongs to a live node

  Scenario: Every message is delivered exactly once
    Given I have created a distributed queue
    And I send 60 messages across 12 groups
    When I drain the queue
    Then I received 60 messages
    And no message was delivered twice

  Scenario: Group order holds on a distributed queue
    Given I have created a distributed queue
    And I send 10 ordered messages to group "cart-42"
    When I drain the queue
    Then the messages of group "cart-42" came back in order

  Scenario: Counts are summed across machines
    Given I have created a distributed queue
    And I send 24 messages across 8 groups
    Then the queue reports 24 ready messages
    And the counts are marked as a point-in-time sum

  Scenario: A single-node queue reports exact counts
    Given I have created a queue
    And I send 12 messages at priority "MEDIUM"
    Then the queue reports 12 ready messages
    And the counts are marked exact
