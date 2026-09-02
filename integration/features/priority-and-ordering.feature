Feature: Priority and ordering
  A single-node queue gives exact priority order and exact FIFO within a
  priority. Messages sharing a group are handed out one at a time, in order,
  whatever their priorities.

  Scenario: Higher priority is delivered first
    Given I have created a queue
    And I send these messages:
      | payload | priority |
      | low-1   | LOW      |
      | high-1  | HIGH     |
      | med-1   | MEDIUM   |
      | high-2  | HIGH     |
    When I drain the queue
    Then the delivery order is "high-1, high-2, med-1, low-1"

  Scenario: FIFO holds within one priority
    Given I have created a queue
    And I send 20 messages at priority "MEDIUM"
    When I drain the queue
    Then the messages come back in the order they were sent

  Scenario: A group is delivered in order regardless of priority
    Given I have created a queue
    And I send these messages:
      | payload | priority | group   |
      | first   | LOW      | order-7 |
      | second  | HIGH     | order-7 |
      | third   | HIGH     | order-7 |
    When I drain the queue
    Then the delivery order is "first, second, third"

  Scenario: A group hands out one message at a time
    Given I have created a queue
    And I send 5 messages to group "user-1"
    When I take 5 messages without acknowledging them
    Then I received 1 message

  Scenario: Acknowledging releases the group
    Given I have created a queue
    And I send 3 messages to group "user-1"
    When I take and acknowledge messages one at a time
    Then I received 3 messages in order

  Scenario: A message not acknowledged comes back
    Given I have created a queue with a 2s visibility timeout
    And I send 1 message at priority "HIGH"
    When I take a message and abandon it
    And I wait 4 seconds
    Then the message can be taken again
    And its attempt count is 2

  Scenario: Nack returns a message immediately
    Given I have created a queue
    And I send 1 message at priority "HIGH"
    When I take a message and nack it
    Then the message can be taken again
