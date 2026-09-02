Feature: Organisations
  The organisation comes from the credential, never the URL. One tenant cannot
  see or touch another tenant's queues even when the names collide.

  Scenario: Two organisations can use the same queue name
    Given I have created a queue as "acme"
    When I create a queue with the same name as "globex"
    Then the request succeeds

  Scenario: Messages do not cross between organisations
    Given I have created a queue as "acme"
    And I have created a queue with the same name as "globex"
    And I send 5 messages at priority "HIGH" as "acme"
    Then "acme" sees 5 ready messages
    And "globex" sees 0 ready messages

  Scenario: One organisation cannot read another's queue
    Given I have created a queue as "acme"
    When "globex" asks for that queue
    Then the request is rejected with 404
