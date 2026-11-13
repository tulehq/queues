#!/bin/sh
# Runs IMAGE the way users do and checks it works: the container must pass its
# own health check, run as the unprivileged user, and serve signed requests:
# create a queue and send and receive a message on it, kept in the store on
# its volume.
set -eu

image=${1:?usage: run.sh IMAGE}
name=queues-smoke-$$

cleanup() {
	status=$?
	if [ "$status" -ne 0 ]; then
		docker logs "$name" || true
	fi
	docker rm -f "$name" >/dev/null 2>&1 || true
	exit "$status"
}
trap cleanup EXIT

docker run -d --name "$name" -p 127.0.0.1::9050 \
	-e TULE_Auth_AccessKeyID=smoke -e TULE_Auth_SecretAccessKey=smoke \
	"$image" >/dev/null

# The health check first runs one interval (10s) after start.
health=
for _ in $(seq 60); do
	health=$(docker inspect -f '{{.State.Status}} {{.State.Health.Status}}' "$name")
	case $health in
	"running healthy") break ;;
	running*) sleep 1 ;;
	*) echo "container stopped: $health" >&2; exit 1 ;;
	esac
done
if [ "$health" != "running healthy" ]; then
	echo "container never became healthy: $health" >&2
	exit 1
fi

# The image must not run as root: it holds the store in its volume.
ids=$(docker exec "$name" id -u):$(docker exec "$name" id -g)
if [ "$ids" != 65532:65532 ]; then
	echo "runs as $ids, want 65532:65532" >&2
	exit 1
fi

addr=$(docker port "$name" 9050/tcp | head -n 1)

# Every call is a signed POST / naming the operation in X-Amz-Target, which is
# what the AWS JSON protocol the SDKs speak looks like on the wire.
sqs() {
	target=$1
	shift
	curl -fsS --aws-sigv4 aws:amz:us-east-1:sqs --user smoke:smoke \
		-H "Content-Type: application/x-amz-json-1.0" \
		-H "X-Amz-Target: AmazonSQS.$target" \
		--data-binary "$1" "http://$addr/"
}

created=$(sqs CreateQueue '{"QueueName":"smoke"}')
case $created in
*'"QueueUrl":"http://'*'/000000000000/smoke"'*) ;;
*)
	echo "created queue $created, want the default account's URL" >&2
	exit 1
	;;
esac
url=http://$addr/000000000000/smoke

sent=$(sqs SendMessage "{\"QueueUrl\":\"$url\",\"MessageBody\":\"hello\"}")
case $sent in
# The MD5 of "hello".
*'"MD5OfMessageBody":"5d41402abc4b2a76b9719d911017c592"'*) ;;
*)
	echo "sent message $sent, want the MD5 of the body" >&2
	exit 1
	;;
esac

received=$(sqs ReceiveMessage "{\"QueueUrl\":\"$url\"}")
case $received in
*'"Body":"hello"'*) ;;
*)
	echo "received $received, want the sent message" >&2
	exit 1
	;;
esac

if ! docker exec "$name" test -s /data/queues.db; then
	echo "no store at /data/queues.db" >&2
	exit 1
fi

echo "smoke test passed: $image"
