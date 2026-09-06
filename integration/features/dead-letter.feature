Feature: Dead-letter and expiry
  A message that fails more times than the queue allows stops being retried, and
  a message that outlives its TTL stops being delivered.

  Scenario: A message that keeps failing is moved to the dead-letter queue
    Given I have created a queue with 2 retries and a dead-letter queue
    And I send 1 message at priority "HIGH"
    # Two retries means two deliveries: the second failure is the last one.
    When I take and nack the message 2 times
    Then the queue reports 1 dead-lettered message
    And the queue holds no ready messages
    And no message is available immediately
    # The counter says it left; this says it arrived.
    And the dead-letter queue holds 1 message

  Scenario: A queue that failures are routed to cannot be deleted
    Given I have created a queue with 2 retries and a dead-letter queue
    Then the dead-letter queue cannot be deleted

  Scenario: Without a dead-letter queue a message is never dropped
    Given I have created a queue with 2 retries and no dead-letter queue
    And I send 1 message at priority "HIGH"
    # There is nowhere to put it, so it keeps coming back rather than vanishing.
    When I take and nack the message 4 times
    Then the queue reports 0 dead-lettered messages
    And the message can be taken again

  Scenario: A delayed message is not delivered before its time
    Given I have created a queue
    When I send a message delayed by 3 seconds
    Then no message is available immediately
    And the queue reports 1 delayed message
    And the message arrives within 8 seconds
