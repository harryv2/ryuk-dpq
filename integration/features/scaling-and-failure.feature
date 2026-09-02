Feature: Scaling and node failure
  Nodes register themselves. Adding one moves work onto it without anybody
  coordinating the move. Losing one is honest about what it costs.

  @scaling
  Scenario: Adding a node moves slots onto it
    Given I have created a distributed queue
    And I send 40 messages across 20 groups
    When I add 2 nodes to the cluster
    Then the queue's slots spread onto the new nodes
    And the queue still holds 40 messages

  @failure
  Scenario: Losing a node makes only its work unavailable
    Given I have created a distributed queue
    And I send 40 messages across 20 groups
    When a node holding some of its slots goes down
    Then the queue still serves messages from the surviving nodes
    And the stats report unavailable slots

  @failure
  Scenario: A restarted node replays its log
    Given I have created a queue
    And I send 15 messages at priority "HIGH"
    When the queue's owner is restarted
    Then the queue still holds 15 messages
    And the messages can still be drained
