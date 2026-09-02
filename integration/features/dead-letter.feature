Feature: Dead-letter and expiry
  A message that fails more times than the queue allows stops being retried, and
  a message that outlives its TTL stops being delivered.

  Scenario: A message that keeps failing is dead-lettered
    Given I have created a queue with 2 retries
    And I send 1 message at priority "HIGH"
    # Two retries means two deliveries: the second failure is the last one.
    When I take and nack the message 2 times
    Then the queue reports 1 dead-lettered message
    And the queue holds no ready messages
    And no message is available immediately

  Scenario: A delayed message is not delivered before its time
    Given I have created a queue
    When I send a message delayed by 3 seconds
    Then no message is available immediately
    And the queue reports 1 delayed message
    And the message arrives within 8 seconds
