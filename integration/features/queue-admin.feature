Feature: Creating and deleting queues
  A queue is created by name and belongs to the organisation the credential
  identifies. Settings are validated before anything is stored, so a queue that
  exists is always one that works.

  Scenario: A queue is created with defaults
    When I create a queue
    Then the queue exists
    And it is a single-node queue
    And it has an owner node

  Scenario: Creating the same queue twice with the same settings is accepted
    Given I have created a queue
    When I create the same queue again
    Then the request succeeds

  Scenario: A queue can be deleted
    Given I have created a queue
    When I delete the queue
    Then the queue is gone

  Scenario Outline: Invalid settings are rejected before anything is stored
    When I create a queue with "<field>" set to "<value>"
    Then the request is rejected with 400
    And the error mentions "<mentions>"

    Examples:
      | field               | value       | mentions            |
      | name                |             | name is required    |
      | name                | bad/name    | letters             |
      | defaultTtl          | soon        | bad duration        |
      | starvationThreshold | 2h          | starvationThreshold |

  Scenario: A dead-letter queue must already exist
    When I create a queue whose dead-letter queue does not exist
    Then the request is rejected with 400
    And the error mentions "does not exist"

  Scenario: A dead-letter queue that exists is accepted
    Given I have created a queue named "dlq"
    When I create a queue with "dlq" as its dead-letter queue
    Then the request succeeds
