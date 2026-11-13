# queues

An Amazon SQS-compatible queue service that keeps its queues in a local
SQLite database. It is built to stand in for SQS in local development and
end-to-end tests: point an AWS SDK at it and the usual queue and message
calls work, standard and FIFO, over both protocols SQS serves, AWS JSON 1.0
and Query.

It is not a production queue service. Every access key it accepts shares
the same queues: the configured key, the account's root user's, and, when it
is pointed at [identity](https://github.com/tulehq/identity), every key
identity issued. Standalone, the configured key may do everything; with
identity, every other key may do what its identity policies and the queue's
policy allow ([Permissions](#permissions)).

## Running it

With Docker:

```sh
docker run --rm -p 9050:9050 -v queuesdata:/data \
  -e TULE_Auth_AccessKeyID=test -e TULE_Auth_SecretAccessKey=test \
  ghcr.io/tulehq/queues:latest
```

The image is based on Alpine, so `docker exec -it <container> sh` gives you a
shell. It runs as the unprivileged `nonroot` user, keeps its database at
`/data/queues.db` on the `/data` volume, and its Docker health check polls
`/_ready`.

From source (Go 1.27.4 or later):

```sh
make build
TULE_Auth_AccessKeyID=test TULE_Auth_SecretAccessKey=test ./build/tule-queues
```

It listens on port 9050, so it does not collide with the other Tule
services: [object-store](https://github.com/tulehq/object-store) on 9000,
[secret-store](https://github.com/tulehq/secret-store) on 9010,
[accounts](https://github.com/tulehq/accounts) on 9020 and identity on 9030.

## Configuration

Settings come from environment variables prefixed with `TULE_`, and from a
`.env` file in the working directory (without the prefix).

| Variable | Default | Purpose |
|---|---|---|
| `TULE_Server_Port` | `9050` | Listen port |
| `TULE_Store_Path` | `./data/queues.db` (`/data/queues.db` in the image) | The SQLite database file; it and its directory are created if missing |
| `TULE_Queues_AccountID` | `000000000000` | The account queue URLs and ARNs carry: 12 digits |
| `TULE_Queues_Region` | `us-east-1` | The region queue ARNs carry |
| `TULE_Queues_BaseURL` | unset | The scheme and host queue URLs start with, such as `http://queues:9050`: an http or https URL with nothing after the host but `/`. Unset, a queue URL starts with the scheme and `Host` the request came in on |
| `TULE_HSTS_MaxAge` | unset | `Strict-Transport-Security` max age; the header is only sent when set |
| `TULE_Server_ReadTimeout` | `30s` | How long the server may take to read a request, body included |
| `TULE_Server_ReadHeaderTimeout` | `30s` | How long it may take to read a request's headers; must be positive |
| `TULE_Server_WriteTimeout` | `30s` | How long it may take to answer, from the end of the request's headers; it must hold a long poll's 20 seconds |
| `TULE_Server_IdleTimeout` | `2m` | How long a keep-alive connection may sit idle between requests |
| `TULE_Server_ShutdownTimeout` | `7s` | How long a graceful shutdown waits for in-flight requests |
| `TULE_Auth_AccessKeyID` | required | The access key clients sign requests with |
| `TULE_Auth_SecretAccessKey` | required | Its secret |
| `TULE_Auth_IdentityEndpoint` | unset | The identity service's base URL, such as `http://localhost:9030`: an absolute http or https URL. Set, the keys identity issued are accepted as well as the configured key, and evaluated against their identity policies, and identity is asked about them with the configured key, which must be identity's root key. Unset, queues runs standalone on the configured key alone |
| `TULE_GasEnv` | | `development` logs console output at trace level, `production` logs at warn level, `testing` disables logging |

The server refuses to start without an access key, because every request is
verified against it and any web page the developer has open can reach a server
on localhost. It also refuses an account ID that is not 12 digits, a region
that is not shaped like an AWS region, and a base URL with a path, query or
fragment.

With an identity endpoint, a request signed with any key but the configured
one is verified through identity's signing-key endpoint, on every request:
a key identity deactivates or deletes is refused on the next one. A request
signed with the configured key never asks identity. When identity cannot be
asked, fails, or gives no answer within 10 seconds, a request signed with
another key fails with `500` `InternalFailure`, never as an unknown key,
and the configured key keeps working.

When the server begins to shut down, the long polls in flight return at
once with no messages, so a shutdown does not wait out their 20 seconds.
Logs go to the console only; there is no log shipping.

## Permissions

Standalone, without `TULE_Auth_IdentityEndpoint`, nothing is evaluated: the
configured key is the account's root user and may call every operation and
the inspection API.

With an identity endpoint, the configured key, which is identity's root key
too, is still never evaluated, as AWS never evaluates the root user. Only the
configured account's root user is root here. Every other key identity issues,
an IAM user's, a role session's, or the root user of another account, is
evaluated on every request against its identity policies as identity hands
them over (a user's inline and attached managed policies, a session's
role's) and the queue's policy, its `Policy` attribute, together, with
[tule.dev/policy](https://github.com/tulehq/policy)'s `Authorize`, as the
IAM User Guide's "Policy evaluation logic" and "Cross-account policy
evaluation logic" describe:

- An explicit `Deny` in either kind of policy denies.
- **Within the configured account**, an `Allow` in either allows. A queue
  policy that names the IAM user, the role (by its ARN, path included, which
  grants its sessions), the session, or `"*"`, is enough on its own; one
  that names only the account (`"000000000000"` or
  `arn:aws:iam::000000000000:root`) delegates to the account and also needs
  an identity `Allow`.
- **Across accounts**, a principal of another account needs both an
  identity `Allow` in its own account and a queue policy `Allow`, through
  its account or directly. Another account's root user has every identity
  permission in its own account, so it needs only the queue policy.
- `AddPermission`, `CreateQueue`, `DeleteQueue`, `ListQueues`,
  `ListQueueTags`, `RemovePermission`, `SetQueueAttributes`, `TagQueue` and
  `UntagQueue` are never shared across accounts: "Cross-account
  permissions don't apply to the following actions" (SQS Developer Guide,
  "Basic examples of Amazon SQS policies", and each one's API reference). A
  principal of another account is evaluated on them without the queue's
  policy, so nothing grants them to it.

A request no policy allows fails with `403` `AccessDenied`, the code SQS
sends in Query and in the JSON protocol's `x-amzn-query-error` header (the
JSON `__type` is `AccessDenied` too), with IAM's message, which names the
policy type that decided:

```
User: arn:aws:iam::000000000000:user/dev is not authorized to perform: sqs:sendmessage on resource: arn:aws:sqs:us-east-1:000000000000:orders because no identity-based policy allows the sqs:sendmessage action
```

The endings are tule.dev/policy's `Reason`:

| Reason | Ending |
|---|---|
| `IdentityDeny` | `with an explicit deny in an identity-based policy` |
| `ResourceDeny` | `with an explicit deny in a resource-based policy` |
| `NoIdentityAllow` | `because no identity-based policy allows the <action> action` |
| `NoResourceAllow` | `because no resource-based policy allows the <action> action` |

When both kinds deny, the identity-based one is named: AWS names "only one
of those policy types" without saying which (IAM User Guide, "Troubleshoot
access denied error messages"). A request that needs an `Allow` of each
kind and has neither is reported for the identity-based type. The action is in lower case, as SQS writes it in its
denials. A denied request does nothing; it is checked before the queue is
looked up, so a queue that does not exist is denied too.

**Breaking change in v0.2.0.** A key identity issued that has no policy
allowing an operation could call it before and is denied now. Give each
user or role the policy it needs, such as `sqs:*` on `*`.

**Changes in v0.3.0.** Queue policies are enforced.

- A queue's `Policy` now grants and denies: a `Deny` in it refuses a
  request the caller's identity policies allow, and a grant naming a user
  or role of the account allows a request its identity policies do not.
- Principals of another account, refused everything in v0.2.0, may now
  call what a queue policy grants them, as above.
- `Policy` is checked when it is set ([Queue policies](#queue-policies)).
  A queue already holding a `Policy` that does not pass, which v0.2.0
  stored without checking, is not evaluated: every evaluated request on it
  fails with `500` `InternalFailure`, logged, until the root key sets a
  valid one or removes it. The root key is unaffected.
- identity v0.4.0 or later is needed: queues asks identity's signing-key
  endpoint for version 3, the principal's attributes. An older identity
  fails every evaluated request with `500` `InternalFailure`.
- `aws:PrincipalArn` for a role session is its role's ARN with the role's
  path, `aws:userid` is supplied for every principal, and
  `aws:PrincipalTag/<key>` is supplied, so a condition on any of them now
  matches as in AWS.

Each operation is evaluated as the action and resource the SQS Service
Authorization Reference gives it (the table is `operationAccess` in
`internal/modules/queues/access.go`, checked by a test against the
reference's JSON in `internal/modules/queues/testdata`):

| Operation | Action | Resource |
|---|---|---|
| `ListQueues` | `sqs:ListQueues` | `*` |
| `CreateQueue` | `sqs:CreateQueue`, and `sqs:TagQueue` when it has tags | the queue it creates |
| `SendMessage`, `SendMessageBatch` | `sqs:SendMessage` | the queue |
| `DeleteMessage`, `DeleteMessageBatch` | `sqs:DeleteMessage` | the queue |
| `ChangeMessageVisibility`, `ChangeMessageVisibilityBatch` | `sqs:ChangeMessageVisibility` | the queue |
| Every other operation | `sqs:<Operation>` | the queue |

A queue is `arn:aws:sqs:<region>:<account>:<name>`, in the configured
account and region. A batch is allowed or denied whole, as its entries all
name its one queue.

The condition keys supplied are `aws:SourceIp` (the connection's address),
`aws:CurrentTime`, `aws:EpochTime`, `aws:SecureTransport`,
`aws:RequestedRegion` (the configured region, as an endpoint fixes it in
AWS, not the region the request was signed for, since any region verifies
here), the principal's keys from the attributes identity hands over
(`aws:PrincipalAccount`, `aws:PrincipalArn`, for a role session its role's
ARN with the role's path, `aws:userid`, `aws:PrincipalType`, and
`aws:username` for an IAM user; a request cannot set them),
`aws:PrincipalTag/<key>` for each of the IAM user's tags or the session's
role's tags (identity issues no session tags), and, from the
reference's keys for SQS: `aws:ResourceTag/<key>` from the queue's tags,
`aws:RequestTag/<key>` and `aws:TagKeys` for `CreateQueue` and `TagQueue`,
and `aws:TagKeys` for `UntagQueue`. They fill policy variables such as
`${aws:username}` too. A request from an evaluated key with more than 50 tags
or tag keys, or an `UntagQueue` key longer than 128 characters, fails with
`InvalidParameterValue` before it is evaluated, which bounds the work an
evaluation does; the root key is not held to it. Every other string evaluated is bounded
before evaluation too: a queue name past 80 characters is refused as the
operation refuses it (`InvalidParameterValue`, or `QueueDoesNotExist` for a
queue URL), so no ARN is longer than the 80-character name allows, and a
tag key or value past 128 or 256 characters fails `InvalidParameterValue`.

A request is decoded once, and the operation acts on the decoding that was
evaluated. The queue an operation acts on is the queue it was evaluated on. Its tags and
its `Policy` are read, with its store ID, in one transaction before the
evaluation, and the operation runs pinned to that ID: a queue deleted and
created again under the same name in between, or created where none
existed, is answered `QueueDoesNotExist` rather than acted on, so the
policy evaluated is the one stored for the queue acted on. A tag or the
policy changed in between is not noticed; the request is evaluated on them
as they were read.

Condition keys match without regard to case, so tag keys that differ only by
case (`env`, `ENV`) would feed one `aws:RequestTag/env` or
`aws:ResourceTag/env`. For an evaluated key, a `CreateQueue` or `TagQueue`
with two such keys, or a `TagQueue` adding one beside a stored key of other
case, fails with `InvalidParameterValue` (checked again in the transaction that
writes the tags, so two such requests at once cannot both pass), and a queue that already holds two
such keys (the root key may write them) is not evaluated: every evaluated
request on it fails with `500` `InternalFailure`, logged, until the root key
removes one.

The [inspection API](#inspection-api) is not AWS's, and needs `tule:Inspect`,
an action of Tule's own, on `arn:aws:sqs:<region>:<account>:*`: an
identity policy grants it with that ARN, a wider pattern such as
`arn:aws:sqs:*:*:*`, or `*`. No queue policy applies to it, so a principal
of another account is always refused it. The configured key needs
nothing.

A principal identity hands over in a state it never produces is not
evaluated, and its request fails with `500` `InternalFailure`, logged:
`Root` set on a principal whose type is not `Account`, or an `Account`
principal without `Root` (this service skips evaluation on the first,
tule.dev/policy grants full identity access on the second, so the two must
agree), or two tags whose keys differ only by case.

Gaps:

- **Permissions boundaries, session policies, SCPs and RCPs** are not
  evaluated; where AWS lets them limit a queue policy's grant to a role, the
  grant here is unlimited.
- **Stored principals are matched by ARN.** AWS turns a user or role ARN in
  a stored resource-based policy into the principal's unique ID, so a
  principal deleted and created again under the same name does not inherit
  the grant. Here it does, until identity can resolve unique IDs for the
  services. A session whose role was deleted is refused everything.
- **`aws:PrincipalOrgID` and `aws:PrincipalOrgPaths` are not supplied**:
  identity does not carry the organization. A positive condition on them
  never holds, and a negated one (`StringNotEquals` and the like) always
  does, so a `Deny` written with a negated operator on them, such as the
  common "deny anyone outside my organization", denies every evaluated
  principal.
- **Service principals never match.** No Tule caller is a service, so a
  statement naming `sqs.amazonaws.com` or `sns.amazonaws.com` grants
  nothing.
- **Event source mappings poll with the credentials functions supplies.**
  The configured key keeps working unchanged; a role's session needs the SQS permissions AWS documents for mappings
  (`sqs:ReceiveMessage`, `sqs:DeleteMessage`, `sqs:GetQueueAttributes`),
  from its identity policies, and from the queue's policy too when the
  role is another account's.

### Queue policies

A queue's policy is its `Policy` attribute, set with `CreateQueue` and
`SetQueueAttributes` and read with `GetQueueAttributes`, and written one
statement at a time by `AddPermission` and `RemovePermission`. It is stored
and returned as it was sent.

`CreateQueue` and `SetQueueAttributes` refuse, with `InvalidAttributeValue`
("Invalid value for the parameter Policy. Reason: ..."), the error SQS
documents for "an incorrect policy or a principal" ("Troubleshoot Amazon
SQS API errors"), a `Policy` that:

- is over a quota of "Amazon SQS policy quotas" (SQS Developer Guide):
  8,192 bytes, 20 statements, 7 actions in a statement, 50 principals or 10
  conditions. How SQS counts principals and conditions is not documented;
  here they are counted across the policy, `"*"` being one principal and
  each key under each operator one condition;
- is not a resource-based policy tule.dev/policy's `ParseResource`
  accepts: valid JSON in the IAM policy grammar, each statement with a
  `Principal` or `NotPrincipal` and a `Resource` or `NotResource`, `AWS`
  principals that are `"*"`, an account ID, or a user, role, session or
  root ARN or unique ID (a group, a partial wildcard or a `Federated`
  principal is refused), unique `Sid`s of any characters. The reason is
  the parser's message; SQS's wording is not documented.

SQS also refuses a principal ARN that names no existing IAM entity; this
service does not ask identity, so such a policy is stored and its
statement matches nobody until the entity exists.

`AddPermission` adds an `Allow` statement whose `Sid` is the label, whose
`Principal` is `{"AWS": [...]}` with `arn:aws:iam::<id>:root` for each of
`AWSAccountIds`, whose `Action` is `SQS:<name>` for each of `Actions`, and
whose `Resource` is the queue's ARN. A queue without a policy gets one with
`Version` `2012-10-17` and `Id` `<queue ARN>/SQSDefaultPolicy`; a policy
already set keeps its `Version`, `Id` and statements, and the new statement
is appended. The SQS documentation does not show the generated statement,
so this shape is not checked against AWS beyond what the API reference
says. The request is checked as the API reference has it: `Label` of 1 to
80 letters, digits, hyphens and underscores, unique in the policy (a label
in use is `InvalidParameterValue`); `AWSAccountIds` of 12 digits, since
"`AddPermission` does not support adding a non-account principal"; at
least one `Actions` value, each `*` or an SQS action name, and at most 7,
else `OverLimit` ("An Amazon SQS policy can have a maximum of seven
actions per statement"). A statement that would put the policy over a
quota is `OverLimit` too ("AddPermission returns this error if the maximum
number of permissions for the queue is reached").

`RemovePermission` removes the statement whose `Sid` is the label, and
fails with `InvalidParameterValue` when the policy has none. Removing the
last statement removes the `Policy`.

## Health checks

`GET /_health` (liveness) and `GET /_ready` (readiness) answer without a
signature. They respond `200` when every service reports healthy or ready and
`503` otherwise, with a JSON object of each service's status. Readiness fails
when the database cannot be reached or its schema is not applied.

## Connecting a client

Sign for the `sqs` service, in any region. One endpoint serves both of SQS's
protocols:

- **AWS JSON 1.0**, which current SDKs speak (aws-sdk-go-v2, boto3 1.34 and
  later, the AWS SDK for JavaScript v3): a signed `POST /` whose
  `X-Amz-Target` header names the operation as `AmazonSQS.<Operation>`, with
  a JSON body that names the queue in `QueueUrl`. `X-Amz-Target` must be
  signed.
- **Query**, which older SDKs and libraries speak: a signed, form-encoded
  `POST` naming the operation in `Action` and the API version in `Version`
  (`2012-11-05`), sent to the queue's URL or to `/`, and answered in XML. A
  request is a Query request when it has no `X-Amz-Target` header. A
  `QueueUrl` parameter, when there is one, names the queue; otherwise the
  URL's path does.

A queue URL is `<scheme>://<host>/<account>/<name>`, and a queue ARN
`arn:aws:sqs:<region>:<account>:<name>`.

Go (AWS SDK v2):

```go
client := sqs.New(sqs.Options{
	Region:       "us-east-1",
	BaseEndpoint: aws.String("http://localhost:9050"),
	Credentials:  credentials.NewStaticCredentialsProvider("test", "test", ""),
})
```

JavaScript (AWS SDK v3):

```js
const client = new SQSClient({
  region: "us-east-1",
  endpoint: "http://localhost:9050",
  credentials: { accessKeyId: "test", secretAccessKey: "test" },
});
```

Gas's queue store, [`gas/queue/sqs`](https://github.com/gasmod/gas), works
unchanged with its `queue.endpoint` set to `http://localhost:9050`, a
`queue.region`, and the access key as `queue.accesskeyid` and
`queue.secretaccesskey`. It takes queue names or the URLs this service
returns.

Or with `curl`, which signs requests too. The JSON protocol:

```sh
curl -fsS --aws-sigv4 aws:amz:us-east-1:sqs --user test:test \
  -H 'Content-Type: application/x-amz-json-1.0' \
  -H 'X-Amz-Target: AmazonSQS.CreateQueue' \
  --data-binary '{"QueueName":"orders"}' http://localhost:9050/

curl -fsS --aws-sigv4 aws:amz:us-east-1:sqs --user test:test \
  -H 'Content-Type: application/x-amz-json-1.0' \
  -H 'X-Amz-Target: AmazonSQS.SendMessage' \
  --data-binary '{"QueueUrl":"http://localhost:9050/000000000000/orders","MessageBody":"hello"}' \
  http://localhost:9050/
```

The Query protocol, to the queue's URL:

```sh
curl -fsS --aws-sigv4 aws:amz:us-east-1:sqs --user test:test \
  -d Action=ReceiveMessage -d Version=2012-11-05 \
  -d MaxNumberOfMessages=10 -d AttributeName.1=All -d MessageAttributeName.1=All \
  http://localhost:9050/000000000000/orders
```

The Query parameter names are the SQS model's: `Attribute.N.Name` and
`Attribute.N.Value` for queue attributes, `Tag.N.Key` and `Tag.N.Value`,
`TagKey.N`, `AttributeName.N` (for both the system attribute names and the
queue attribute names), `MessageAttributeName.N`,
`MessageAttribute.N.Name` with `MessageAttribute.N.Value.DataType`,
`.StringValue` and `.BinaryValue` (base64), `MessageSystemAttribute.N...`,
and `SendMessageBatchRequestEntry.N.<Member>` and its peers for batches. The
one-item forms without an index that the API reference's examples show
(`AttributeName=All`, `Attribute.Name=...`, `Tag.Key=...`) are read too.
`MessageSystemAttributeName.N` is read as well as `AttributeName.N`.

## Supported operations

| Operation | Notes |
|---|---|
| `CreateQueue` | Standard, or FIFO for a name ending `.fifo` with `FifoQueue` `true`. Idempotent: the same name with every attribute the request sets equal to the queue's returns the queue's URL, and another value fails with `QueueNameExists` (`QueueAlreadyExists`). Tags are set on creation |
| `DeleteQueue` | Deletes the queue and its messages at once |
| `GetQueueUrl` | |
| `ListQueues` | `QueueNamePrefix`, `MaxResults` 1 to 1000, and `NextToken` only when `MaxResults` is set; without it, up to 1000 URLs |
| `GetQueueAttributes` | `All` or named attributes, the approximate counts included |
| `SetQueueAttributes` | Every settable attribute; an empty `Policy`, `RedrivePolicy`, `RedriveAllowPolicy` or `KmsMasterKeyId` removes it. `Policy` is checked ([Queue policies](#queue-policies)) |
| `PurgeQueue` | Deletes every message, in flight or not, at once |
| `TagQueue` / `UntagQueue` / `ListQueueTags` | |
| `ListDeadLetterSourceQueues` | Paginated as `ListQueues` |
| `AddPermission` / `RemovePermission` | Write and remove a statement in the queue's `Policy` ([Queue policies](#queue-policies)) |
| `SendMessage` / `SendMessageBatch` | Delays, message attributes of every type, `AWSTraceHeader`, FIFO groups and deduplication; batches report each entry |
| `ReceiveMessage` | `MaxNumberOfMessages`, `VisibilityTimeout`, `WaitTimeSeconds` long polling, `MessageAttributeNames` (`All`, `.*`, `prefix.*`), `MessageSystemAttributeNames` and the deprecated `AttributeNames`, `ReceiveRequestAttemptId` on FIFO queues |
| `DeleteMessage` / `DeleteMessageBatch` | |
| `ChangeMessageVisibility` / `ChangeMessageVisibilityBatch` | |

The queue attributes are SQS's: `DelaySeconds` (0 to 900, default 0),
`MaximumMessageSize` (1024 to 1048576, default 1048576),
`MessageRetentionPeriod` (60 to 1209600, default 345600),
`ReceiveMessageWaitTimeSeconds` (0 to 20, default 0), `VisibilityTimeout`
(0 to 43200, default 30), `RedrivePolicy` (`deadLetterTargetArn`, and
`maxReceiveCount` 1 to 1000, default 10, as a number or a string),
`RedriveAllowPolicy` (`allowAll`, `denyAll`, or `byQueue` with 1 to 10
`sourceQueueArns`), `Policy`, `KmsMasterKeyId`,
`KmsDataKeyReusePeriodSeconds` (60 to 86400), `SqsManagedSseEnabled`
(default `true`), and on a FIFO queue `FifoQueue`,
`ContentBasedDeduplication` (default `false`), `DeduplicationScope`
(`queue`, the default, or `messageGroup`) and `FifoThroughputLimit`
(`perQueue`, the default, or `perMessageGroupId`, which needs
`messageGroup`). `GetQueueAttributes` also reports `QueueArn`,
`CreatedTimestamp` and `LastModifiedTimestamp` (epoch seconds) and the
three approximate counts. A name SQS does not have, a FIFO attribute on a
standard queue, and `FifoQueue` after creation fail with
`InvalidAttributeName`; a value out of range with `InvalidAttributeValue`.

Messages are held to SQS's rules:

- A body is 1 character or more, of the characters SQS allows (`#x9`,
  `#xA`, `#xD`, `#x20` to `#xD7FF`, `#xE000` to `#xFFFD`, `#x10000` to
  `#x10FFFF`, else `InvalidMessageContents`).
- A message is at most the queue's `MaximumMessageSize`, counting the body
  and every attribute's name, type and value.
- A message has up to 10 attributes. Their names, types and values follow
  the developer guide's rules: `String`, `Number` (38 digits, 10^-128 to
  10^+126) or `Binary`, with an optional custom suffix, and names without
  the `AWS.` and `Amazon.` prefixes.
- A batch has 1 to 10 entries with distinct, well-formed IDs, and at most
  1 MiB of messages in all.

`MD5OfMessageBody`, `MD5OfMessageAttributes` and
`MD5OfMessageSystemAttributes` are computed as the SQS developer guide
gives the algorithm, and match the API reference's examples.

A FIFO receive reads the queue's groups, not its messages: one indexed
seek per group finds the groups with nothing in flight, so a long backlog
behind a message in flight costs nothing, and a queue's cost grows with its
number of groups only. A standard receive reads only the messages it
takes. Neither grows with the rest of the queue.

A long poll returns as soon as it has one message. It waits on a signal
that a send, a visibility change, a delete or a dead-letter move raises,
and on a timer for the next delayed or in-flight message, not by polling
the store.

On a FIFO queue, `MessageGroupId` is required. Every message has a
deduplication ID: the request's `MessageDeduplicationId`, or with
`ContentBasedDeduplication` the SHA-256 of the body. A message sent again
with an ID the queue saw within 5 minutes, in the queue or in the group as
`DeduplicationScope` says, is accepted and not stored, and the reply
carries the first message's ID and sequence number. Messages are received
in order within a group, and a group is held while any of its messages is
in flight. Each stored message gets the next `SequenceNumber`.

A message whose receive count has reached its queue's `maxReceiveCount` is
moved to the dead-letter queue instead of being received again. The
dead-letter queue must exist, be of the same type, not be the queue
itself, and allow the queue in its `RedriveAllowPolicy`, else the
`RedrivePolicy` fails with `InvalidParameterValue`.

## Inspection API

Beside SQS's API, the service answers JSON routes under `/_queues/` that are
not AWS's: to look at the queues and their messages without receiving them,
and to reset the store between tests. They are signed like every other
request, for the `sqs` service, with the configured key or any key identity
accepts, and a key identity issued needs `tule:Inspect`
([Permissions](#permissions)).

| Route | Answer |
|---|---|
| `GET /_queues/queues` | `200` with `{"items": [queue, ...]}`: every queue, in name order, not paginated |
| `GET /_queues/queues/{name}/messages` | `200` with `{"items": [message, ...], "nextCursor": "..."}`: the queue's messages in queue order, without changing their visibility or receive counts. `state` (`delayed`, `visible` or `inFlight`) and `group` filter it; `limit` is 1 to 100, 20 by default; `cursor` takes the `nextCursor` of the page before, which the last page leaves out |
| `DELETE /_queues/messages` | `204`: deletes every message of every queue, with the FIFO deduplication and receive-attempt records; the queues are kept. It is not held to `PurgeQueue`'s 60 seconds |
| `DELETE /_queues/queues` | `204`: deletes every queue. Their names can be created again at once |

A queue is `{"name", "url", "arn", "attributes", "counts"}`, where
`attributes` is what `GetQueueAttributes` reports for `All`, as strings,
and `counts` is `{"visible", "inFlight", "delayed"}`.

A message is:

```json
{
  "id": "4144678e-5490-40d3-8660-480927d87d74",
  "body": "hello",
  "attributes": {"SenderId": "test", "SentTimestamp": "1794539309572",
                 "ApproximateReceiveCount": "1",
                 "ApproximateFirstReceiveTimestamp": "1794539309590"},
  "messageAttributes": {"kind": {"dataType": "String", "stringValue": "greeting"}},
  "sentTime": "2026-11-13T03:08:29.572Z",
  "state": "inFlight",
  "visibleAt": "2026-11-13T03:08:59.590Z",
  "receiveCount": 1,
  "group": "g",
  "deduplicationId": "d",
  "sequenceNumber": "00000000000000000001"
}
```

`attributes` are the system attributes `ReceiveMessage` would report for
`All`; a Binary message attribute has a base64 `binaryValue`. `group`,
`deduplicationId` and `sequenceNumber` are left out when the message has
none. Times are RFC 3339 in UTC, to the millisecond. `visibleAt` is when a
delayed or in-flight message becomes visible, or when a visible one did.

An error is `{"code", "message"}` with the code in `X-Amzn-ErrorType`: `404`
`QueueDoesNotExist` for an unknown queue, `400` `InvalidParameterValue` for
a bad `state`, `limit` or `cursor`, and the signature errors of SQS's API.

```sh
curl -fsS --aws-sigv4 aws:amz:us-east-1:sqs --user test:test \
  http://localhost:9050/_queues/queues

curl -fsS --aws-sigv4 aws:amz:us-east-1:sqs --user test:test \
  'http://localhost:9050/_queues/queues/orders/messages?state=visible&limit=10'

curl -fsS --aws-sigv4 aws:amz:us-east-1:sqs --user test:test \
  -X DELETE http://localhost:9050/_queues/messages

curl -fsS --aws-sigv4 aws:amz:us-east-1:sqs --user test:test \
  -X DELETE http://localhost:9050/_queues/queues
```

## Differences from AWS

- **The queues are the configured account's.** What a key may do is
  decided by its identity policies and the queue's policy
  ([Permissions](#permissions)). A principal of another account has no
  queues of its own here: it cannot create one (`CreateQueue` is not
  shared across accounts), and reaches this account's queues as far as
  their policies let it.
- **One account and one region.** The configured ones. A queue URL of
  another account, and `GetQueueUrl` with another
  `QueueOwnerAWSAccountId`, fail with `QueueDoesNotExist`, before the
  request is evaluated; a request is always evaluated on the ARN of the
  configured region and account and the queue's name. A queue URL's
  host is not checked, since requests go to whatever endpoint the client
  is configured with.
- **No encryption.** `KmsMasterKeyId`, `KmsDataKeyReusePeriodSeconds` and
  `SqsManagedSseEnabled` are checked, kept and reported, and change
  nothing; no KMS error is ever returned.
- **Standard queues are exact.** They deliver every message once per
  receive, in the order messages became visible (a message never received
  in the order it was sent), and a short poll sees every visible message,
  where SQS samples its servers and may deliver out of order or twice.
  `MessageGroupId` is accepted on a standard queue and reported, but there
  is no fair queuing.
- **Nothing is throttled or partitioned.** `RequestThrottled` is never
  returned, and the high-throughput FIFO attributes change nothing but
  `DeduplicationScope`. No quota is enforced past the per-request limits:
  the 120,000 in-flight messages SQS allows are not counted, since counting
  them would make every receive read every message in flight, so
  `ReceiveMessage` never answers `OverLimit`.
- **Deletes and purges are immediate.** `DeleteQueue` and `PurgeQueue` take
  effect at once, where SQS takes up to 60 seconds; the 60 seconds are kept
  as SQS's errors: a name deleted less than 60 seconds ago fails
  `CreateQueue` with `QueueDeletedRecently`, and a second purge within 60
  seconds fails with `PurgeQueueInProgress`.
- **Message move tasks are not served.** `StartMessageMoveTask`,
  `CancelMessageMoveTask`, `ListMessageMoveTasks`, and any other operation
  not listed above, answer as an action SQS does not know: `400`
  `InvalidAction`, over either protocol.
- **Choices where AWS documents none.**
  - Receipt handles are opaque base64 and name one receive. A
    `DeleteMessage` with an earlier receive's handle succeeds and deletes
    nothing, as the API reference allows; one whose message is already
    gone succeeds too.
  - A `ChangeMessageVisibility` whose receive is over, because its
    timeout ended or the message was received again, fails with
    `MessageNotInflight`; one whose message is gone, with
    `ReceiptHandleIsInvalid`.
  - A visibility timeout that would keep a message in flight past 12 hours
    from its receive fails with `InvalidParameterValue`.
  - A FIFO message deduplicated on send is reported with the first
    message's `MessageId` and `SequenceNumber`, and the digests of the
    request just sent.
  - A FIFO send may carry `DelaySeconds` 0; any other per-message delay
    fails with `InvalidParameterValue`. A FIFO queue's `DelaySeconds`
    applies to messages sent after it is set, where SQS applies it to
    those already queued as well.
  - Sequence numbers are 20-digit decimal strings that count up by one per
    message of the queue.
  - A message moved to a dead-letter queue goes to its back, visible, keeps
    its receive count and gets the source queue's ARN as
    `DeadLetterQueueSourceArn`; a standard message keeps its enqueue time,
    a FIFO one starts its retention again, as the developer guide
    describes. When the dead-letter queue no longer exists, the message
    stays and is received.
  - A retried `ReceiveRequestAttemptId` returns the same messages and
    handles while none was deleted or received again; a visibility change
    in between is not counted as a change.
  - `CreateQueue` compares only the attributes the request sets, and not
    the tags.
  - The number of tags is not limited; SQS recommends at most 50.
  - A Query form that could be read two ways fails with
    `MalformedQueryString`, before it is evaluated: a list index spelled
    other than `1`, `2`, ... (`Tag.01`, `Tag.+1`, `Tag.0`), and a list
    given both indexed and in the one-item shorthand (`Tag.1.Key` with
    `Tag.Key`). A parameter given more than once is read as its first
    value.
  - A `CreateQueue` or `TagQueue` that gives one tag key two values, as
    two JSON members (an escaped spelling counts as the same key) or two
    `Tag.N.Key` entries, fails with `InvalidParameterValue`, for every
    key, before the request is evaluated, rather than the later value
    winning.
  - `PurgeQueue` keeps a FIFO queue's deduplication records, as deleting
    a message does, which the developer guide documents: a message sent
    again within 5 minutes with a purged message's deduplication ID is
    accepted and not stored. AWS documents nothing either way for a purge.
    The inspection API's `DELETE /_queues/messages` clears them.
  - `SenderId` is the access key ID that signed the send.
  - Missing parameters fail with `MissingParameter`, a malformed JSON body
    with `SerializationException`, and a malformed form with
    `MalformedQueryString`.
- **Error statuses are the SDK model's.** Each SQS error is answered with
  the status its `httpError` and `awsQueryError` traits give in the SDK
  model (`aws-sdk-go-v2/service/sqs` `schemas/schemas.go`): `404` for
  `ReceiptHandleIsInvalid` and `InvalidAddress`, `403` for
  `PurgeQueueInProgress` and `OverLimit` (sent for an `AddPermission` of
  more than seven actions), `400` for the rest. The API
  reference lists `400` for every per-action error. The common errors take
  the common errors reference's statuses. The SDKs read the code, not the
  status.
- **Error shapes.** A JSON error's `__type` is the SQS shape, namespaced
  (`com.amazonaws.sqs#QueueDoesNotExist`), with `message`, and
  `x-amzn-query-error` carries the Query code and fault
  (`AWS.SimpleQueueService.NonExistentQueue;Sender`), which the SDKs report
  as the error code, as the developer guide shows. An error the SQS model
  has no shape for, such as `InvalidParameterValue`, has its bare code as
  `__type`. A Query error is an `ErrorResponse` with `Type`, `Code`,
  `Message` and `RequestId`, without a namespace, as the developer guide
  shows it. An operation with no result answers JSON with an empty body.
- **Query requests are POSTs.** A GET with the parameters in the query
  string is not served. `Version`, when sent, must be `2012-11-05`.
- **Request bodies are at most 4 MiB.** SQS takes 1 MiB of messages per
  request; the request carrying them can be larger once escaped, and 4 MiB
  holds the largest batch however it is encoded.
- **Timestamps have millisecond precision.**
- **Signature verification is request-level,** by
  [tule.dev/sigv4](https://github.com/tulehq/sigv4). The method, path,
  query, signed headers and the body's hash are checked, along with the
  credential's access key and service (`sqs`) and a 15-minute clock skew.
  A signature must cover `host` and `x-amz-date`, `x-amz-target` on a JSON
  request, and every `x-amz-*` header the request carries, and the
  `SignedHeaders` list must be lowercase, sorted and unique. The path is
  canonicalized as every service but S3 signs it. It must carry a real
  SHA-256 of the body: `UNSIGNED-PAYLOAD` is rejected. Failures answer with
  the Query codes: `MissingAuthenticationToken`, `IncompleteSignature`,
  `SignatureDoesNotMatch`, `InvalidClientTokenId` and `ExpiredToken`.
- **The credential's region is not checked.** It is signed, but any region
  verifies.
- **Session tokens need identity.** The configured key is long-term, so a
  request signed with it and carrying `X-Amz-Security-Token` fails with
  `InvalidClientTokenId`. With identity, a role session's key verifies with
  its token; an expired one fails with `ExpiredToken`.
- **One process per database.** The store is one SQLite file reached through
  one connection, which serializes the server's transactions, and long
  polls wait on a signal inside the process; nothing coordinates two
  servers pointed at the same file.
- **Logs record the connection's peer address.** `X-Forwarded-For` is not
  trusted, since nothing is expected in front of this service and the header
  is unsigned.

## Development

```sh
make test       # unit tests
make test-e2e   # end-to-end tests: the app in-process, driven by the AWS SDK
make lint
make image      # build ghcr.io/tulehq/queues:dev
make smoke      # build the image, run it, and check it serves signed requests
make push VERSION=0.1.0   # build for amd64 and arm64 and push
```

Tests can run the queues on a clock of their own:
`(*app.Config).SetClock(now func() time.Time)`, called between
`app.LoadConfigFrom` and `app.New`, drives delays, visibility timeouts,
retention, the FIFO windows and the purge and delete windows, while
signatures are still checked against the real time. A long poll already
waiting does not see the clock move: move it, then receive.

The end-to-end tests include ones against a real identity service, built
from an identity checkout and run as a process pointed back at the app under
test, and, for role sessions, a real accounts service that identity's
`AssumeRole` asks about member accounts. The checkouts are the siblings
`../identity` and `../accounts` by default, or the directories
`TULE_E2E_IDENTITY_DIR` and `TULE_E2E_ACCOUNTS_DIR` name; CI checks out
`tulehq/identity` v0.4.0 and `tulehq/accounts` v0.2.0 and sets them.
Without a checkout those tests fail rather than being skipped.

## License

[MIT](LICENSE)
