#!/usr/bin/env bash
# Scripted QueueServer V2 demo. Each step prints the command, waits for Enter
# (unless AUTO=1), then runs it. Requires the controller to be running:
#
#   pixi run -e py312 python demo/setup.py
#   pixi run -e py312 start-qserver-v2 --config demo/state/controller.yml --port 8443
#
# then, in another terminal:  bash demo/walkthrough.sh

set -u
cd "$(dirname "$0")/.."
qsv2() { pixi run -e py312 python demo/qsv2.py "$@"; }

step() {
    printf '\n\033[1;36m[%s]\033[0m \033[1m%s\033[0m\n' "$1" "$2"
    shift 2
    printf '\033[2m$ qsv2 %s\033[0m\n' "$*"
    if [ -z "${AUTO:-}" ]; then read -r -p "" _; fi
    qsv2 "$@"
}

note() { printf '\n\033[33m%s\033[0m\n' "$*"; }

step 1 "Readiness is component-level, not 'process is up'" ready
step 2 "A token signed by an unknown key is rejected before any handler runs" --as forged queue
step 3 "Read scope cannot mutate" --as viewer submit
step 4 "Take control: server-owned lease bound to the authenticated subject" lease acquire --ttl 600
step 5 "Another principal cannot edit while the lease is held" --as admin submit
step 6 "Out-of-schema parameters are refused (422) before persistence" submit --raw \
    '{"operation_id":"simulated-count","operation_version":"1","parameters":{"detectors":["det"],"num":11}}'
step 7 "There is no generic execute-plan operation to call" submit --raw \
    '{"operation_id":"execute_plan","operation_version":"1","parameters":{"name":"count","args":[["det"]]}}'
step 8 "Submit two reviewed operations" submit --num 2 --delay 0.5
qsv2 submit --num 3 --delay 0.5 | head -3
step 9 "A stale queue revision is refused (412); nothing was written" --stale submit
step 10 "Start queue execution: admitted work is recorded durably" start
note "Switch to the terminal running 'qsv2 stream' to watch claimed -> running -> succeeded."
step 11 "Replay with the same Idempotency-Key returns the stored response" --key demo-renew lease renew --ttl 600
qsv2 --key demo-renew lease renew --ttl 600 | head -2
step 12 "Same key, different body: 409 conflict" --key demo-renew lease renew --ttl 900

note "---- Failure path ----"
note "(waiting for the running execution to finish)"
sleep 5
step 13 "Queue a 20 s operation and start it" submit --num 10 --delay 2
qsv2 start | head -3
sleep 3
step 14 "SIGKILL the worker while the RunEngine is mid-plan" kill-worker
sleep 3
step 15 "Outcome is 'unknown'; dispatch is blocked; readiness is 503" ready
step 16 "Nothing was retried or requeued; the execution is blocked" queue
step 17 "Operator acknowledges after investigation; fence evidence was recorded automatically" \
    ack --note "worker SIGKILLed during demo; simulator only"
step 18 "Ready again; remaining queued work is NOT resumed automatically" ready

note "---- Restart ----"
note "Now restart the controller process, then run:  qsv2 events --after 0 --limit 200"
note "Every operation, attempt, lease and event is still there."
