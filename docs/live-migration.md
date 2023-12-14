# Chorus s3 live migration

[Edit link](https://www.planttext.com/?text=tLPDRzim3BtxLn3fAK1JeCqs80OMT1iMxB1Y0thOeUb1KHZNZIovajmq_twKB7-eZjkTTqaiy_6HUgHeNHlBjAqAaIGsjm9XSZi7aJyZ57cccSsLd2G9urPfc7ohZ5K5wWam0pmiaf88SfwNJ5g2F2XTcRNyhTNBqUDAzmHdYyLgjLZ4uDlvMjwmN7fSGGzmjbfzkPhFXx2L4a8TGkXTMBJmBRDimmnsyM_GClUsfgP5ybHXHO3lpoZjMlgLDw9xyTIOVsqUWQlos4UepMDa5xc96Dow1Noj-1uZyyQ_UJgjqn-bzs5N1__OAfKacLPL2J-Ot8fSPg1afjo_cOD6eZVM9D2MORRqGgTmEOM3pYqs8C9uospf54xXAh9x3cTe_NqTD9fIIOCzPz8mXVaK-0FoFUIxha70zn1k7HEXPiiN25nIsSO8CtfI9T1kYDrMMWuYrSwDRqSQPV1X2bL9bKLNbfhY6RMXD3faNT8KFbAs6XtMChFlx8rcANoQdau6vRRH_zRbyUf8ZEZkKuvGKUfNFV569ME2CjuU1u2-AgzBIbLnFHWwpNJcq7fUjQ14_SeBOlhR_1bj4Eu2pWM_9yB0D5lMNPgIL1oTIu-FOBLrjcnEQ1g7xI7gp3DiiWR6nNcfHCwFvoBV8JzoWHSN5wT9wOJ-kvy_dsqcFQLS2o6tfix-VJ6yQ_1nedZYeIIZ4NgxzmtXTYyOWQbS78sXK-AQMh821qhrOyDEgsB0PkfmxPc0pt3P0xafregWmfWxdLSuO6EflI6KZzyidKGkwb2XjyFNXyHlevtqpYESStEoYmWmtYgnCNBgzmzQFiunYelLRpDkAl26MfnP_a1gquwPpD3tL3sxJalcRV-_Paq3HrFua9DMOtEtCOwEsqvqhefIuBjVAxhkK19aoEYRO5vFuJv9Y1aiD8hlLEaYCN4F6w4eVlGvNijAqaKhrBA-D61HscD7rOb8OvjsJyuHHPZRr_HB5xU_)

![Static image](https://www.planttext.com/api/plantuml/png/tLPDRzim3BtxLn3fAK1JeCqs80OMT1iMxB1Y0thOeUb1KHZNZIovajmq_twKB7-eZjkTTqaiy_6HUgHeNHlBjAqAaIGsjm9XSZi7aJyZ57cccSsLd2G9urPfc7ohZ5K5wWam0pmiaf88SfwNJ5g2F2XTcRNyhTNBqUDAzmHdYyLgjLZ4uDlvMjwmN7fSGGzmjbfzkPhFXx2L4a8TGkXTMBJmBRDimmnsyM_GClUsfgP5ybHXHO3lpoZjMlgLDw9xyTIOVsqUWQlos4UepMDa5xc96Dow1Noj-1uZyyQ_UJgjqn-bzs5N1__OAfKacLPL2J-Ot8fSPg1afjo_cOD6eZVM9D2MORRqGgTmEOM3pYqs8C9uospf54xXAh9x3cTe_NqTD9fIIOCzPz8mXVaK-0FoFUIxha70zn1k7HEXPiiN25nIsSO8CtfI9T1kYDrMMWuYrSwDRqSQPV1X2bL9bKLNbfhY6RMXD3faNT8KFbAs6XtMChFlx8rcANoQdau6vRRH_zRbyUf8ZEZkKuvGKUfNFV569ME2CjuU1u2-AgzBIbLnFHWwpNJcq7fUjQ14_SeBOlhR_1bj4Eu2pWM_9yB0D5lMNPgIL1oTIu-FOBLrjcnEQ1g7xI7gp3DiiWR6nNcfHCwFvoBV8JzoWHSN5wT9wOJ-kvy_dsqcFQLS2o6tfix-VJ6yQ_1nedZYeIIZ4NgxzmtXTYyOWQbS78sXK-AQMh821qhrOyDEgsB0PkfmxPc0pt3P0xafregWmfWxdLSuO6EflI6KZzyidKGkwb2XjyFNXyHlevtqpYESStEoYmWmtYgnCNBgzmzQFiunYelLRpDkAl26MfnP_a1gquwPpD3tL3sxJalcRV-_Paq3HrFua9DMOtEtCOwEsqvqhefIuBjVAxhkK19aoEYRO5vFuJv9Y1aiD8hlLEaYCN4F6w4eVlGvNijAqaKhrBA-D61HscD7rOb8OvjsJyuHHPZRr_HB5xU_)
```plantuml
@startuml

title "S3 live migration"

actor "Customer" as customer
participant "Chorus\nProxy" as proxy #99FF99
participant "S3\nMain" as main #FFA233
participant "S3\nFollower" as follower #FFA233
database "Chorus\nMetadata" as meta
queue "Event\nqueue" as eventq
queue "Obj copy\nqueue" as objq
queue "Obj list\nqueue" as listq
queue "Bucket\nqueue" as bucketq
participant "Chorus\nWorker" as worker #99FF99


group Handling ongoing s3 requests
  customer->proxy: 1) write request
  proxy->main: 2) write requets
  main->proxy: main response
  proxy->meta: 3) check if migration started
  meta->x proxy: not started - stop and return main response
  meta-> proxy: started
  proxy->meta: 4) update obj main version
  proxy->eventq: 5) create task
  proxy->customer: 6) return main response
end

group Start live migration
  customer->proxy: 1) start migration
  proxy->meta: 2) check if started
  meta->x proxy: already started
  meta->proxy: start
  proxy->main: 3) list buckets
  proxy->bucketq: 4) create bucket tasks
end

group Live migration [bucket meta]
  bucketq->worker: receive bucket task
  worker->follower: create bucket
  worker->main: get bucket acl,policy,lifecycle...
  worker->follower: copy bucket acl,policy,lifecycle...
  worker->listq: create list bucket objects task
end

group Live migration [bucket list objects]
  listq->worker: receive list bucket objects task
  worker->meta: get last processed obj name
  worker->main: list objects from
  worker->meta: set obj main ver = 1
  worker->objq: create obj copy task
  worker->meta: update last processed obj name
end

group Live migration [obj migration]
  objq->worker: receive obj copy task
  worker->meta: check main follower obj versions
  worker->main: get obj
  worker->follower: copy obj
  worker->main: get obj acl,policy,etc
  worker->follower: copy acl,policy,etc
  worker->meta: set obj follower ver = 1
end

group Live migration [catch up changes]
  eventq->worker: receive obj event task
  worker->meta: check main follower obj versions
  worker->main: get obj
  worker->follower: copy obj
  worker->main: get obj acl,policy,etc
  worker->follower: copy acl,policy,etc
  worker->meta: set obj follower ver = main ver
end

group Switch
  customer->proxy: 1) switch
  proxy->meta: 2) check if migration completed
  meta->x proxy: not started or in progress
  meta->proxy: completed
  proxy->proxy: 3) [optional] block write request\nunitl event queue empty
  proxy->follower: 4) route write requests
end

@enduml
```

## Switch main<->follower

### Sequence diagram
[Edit link](https://www.planttext.com/?text=bLPDSzem4BtxL-p09Pwf3mtzWYbCCabeEzCIfZ2JIox2ba55IAuaX_3lkrgCZR6PafjjFUs-VK-xyerrpBXy8uF02ISvT6OzaEA5mqOi3ND2gquGiDXf0vsxt3gzuQO3p49Sl0GP1X2no9Xo25bfazjdDJNwTKUup3z1jzyVZ_lzEdZMUrQFCY6OxMcPG7SylhtkzPgm2TyMCCMt9InXZYsOvLNUNzmn_vN06tm9_kOyHy2SsVMpeXTQewTsxa_Qh8iYj_HOiW-2uH0UVzw3mE1Cg7SWb722oKejIBJYC1m6mTBeF8Ej4Ow3uPZFEaYbtWPGQXUDI9q1l0_hGCJGIZGYNGPmtGJGIXd1S9jfPNcrqrS_W5w876F3lKfwyQVG-OKRYsGhCCanW0yXU2pcSQWNK08KlC8Tc0_WOuWvNMxKSMeA4utsiWtWK-ZtnTnQYeU0_Oel2yCEu7C8TgTYp-neSS_yIuEvj_x0dAk4t9XjXOjNtfIlqMZoyEIbRyXhzw23TZM1DioiOP77Q-vWzlHZVlSzcirlVyyVxeFp7W9B7PwBVTot6agLjngAAvSOccU9jwWfod_ifF3U_YkT-RFAP7YXiwHseGO6JDuinl6cqufGYYMARtkmsA4ThrHOKH948xYYCnkszSH9s3CjGUptv0ivDhbqmZS_QYarnhYeZZER6zwUnzKjBdABv5GHp6jujOnzGBCCKfp2IhUsOnNhgDhY8t6ugCAZ3QUSuXMFro3I9gs0IGVTAMVnQfgdAJ9ieHuTlkt5IM31KrzaoSMZIXvADzokKpldjCHXqPqooRyPpjKbF4uagT7OSTimzvH7Qyzw3rfCQksd5EyGkweO7APWqSE9mOi6c9JbDAPcjhLXIWfb6a7-Y2HCBQLGoxOJM9-nDFoAgNV_E7aen-f4cmrUQtHe5hahvannxVapI5TSS8Du_7du1m00)

![Static image](https://www.planttext.com/api/plantuml/png/bLPDSzem4BtxL-p09Pwf3mtzWYbCCabeEzCIfZ2JIox2ba55IAuaX_3lkrgCZR6PafjjFUs-VK-xyerrpBXy8uF02ISvT6OzaEA5mqOi3ND2gquGiDXf0vsxt3gzuQO3p49Sl0GP1X2no9Xo25bfazjdDJNwTKUup3z1jzyVZ_lzEdZMUrQFCY6OxMcPG7SylhtkzPgm2TyMCCMt9InXZYsOvLNUNzmn_vN06tm9_kOyHy2SsVMpeXTQewTsxa_Qh8iYj_HOiW-2uH0UVzw3mE1Cg7SWb722oKejIBJYC1m6mTBeF8Ej4Ow3uPZFEaYbtWPGQXUDI9q1l0_hGCJGIZGYNGPmtGJGIXd1S9jfPNcrqrS_W5w876F3lKfwyQVG-OKRYsGhCCanW0yXU2pcSQWNK08KlC8Tc0_WOuWvNMxKSMeA4utsiWtWK-ZtnTnQYeU0_Oel2yCEu7C8TgTYp-neSS_yIuEvj_x0dAk4t9XjXOjNtfIlqMZoyEIbRyXhzw23TZM1DioiOP77Q-vWzlHZVlSzcirlVyyVxeFp7W9B7PwBVTot6agLjngAAvSOccU9jwWfod_ifF3U_YkT-RFAP7YXiwHseGO6JDuinl6cqufGYYMARtkmsA4ThrHOKH948xYYCnkszSH9s3CjGUptv0ivDhbqmZS_QYarnhYeZZER6zwUnzKjBdABv5GHp6jujOnzGBCCKfp2IhUsOnNhgDhY8t6ugCAZ3QUSuXMFro3I9gs0IGVTAMVnQfgdAJ9ieHuTlkt5IM31KrzaoSMZIXvADzokKpldjCHXqPqooRyPpjKbF4uagT7OSTimzvH7Qyzw3rfCQksd5EyGkweO7APWqSE9mOi6c9JbDAPcjhLXIWfb6a7-Y2HCBQLGoxOJM9-nDFoAgNV_E7aen-f4cmrUQtHe5hahvannxVapI5TSS8Du_7du1m00)

```plantuml
@startuml

title "S3 live migration"

actor "Customer" as customer
participant "Chorus\nProxy" as proxy #99FF99
participant "S3\nOld" as s3old #FFA233
participant "S3\nNew" as s3new #FFA233
database "Chorus\nMetadata" as meta
queue "Task\nqueue" as queue
participant "Chorus\nWorker" as worker #99FF99


== OLD is main, initial migration done ==

group write request flow
  customer->proxy: 1) write request
  proxy->s3old: 2) write request
  s3old->proxy: response
  proxy->meta: 3) increase obj s3old version
  proxy->queue: 4) create task old->new
  proxy->customer: 5) return response
  queue->worker: 6) process task
  worker->s3new: 7) sync obj
  worker->meta: 8) increase obj s3new version
end

== Switch OLD<->NEW ==
customer->proxy: switch request
proxy->meta: mark bucket SWITCH-STARTED


group write request flow after switch
  customer->proxy: 1) write request
  proxy->s3new: 2) write request
  s3new->proxy: response
  proxy->meta: 3) update obj s3new version
  proxy->queue: 4) create task new->old (optional)
  proxy->customer: 5) return response
end


group read request flow after switch
  customer->proxy: 1) read request
  proxy->meta: 2) get s3 by max obj version -> (s3old)
  proxy->s3old: 2) read request
  s3old->proxy: response
end

group create multipart upload flow after switch
  customer->proxy: create multipart upload
  proxy->s3new: create multipart upload
  s3new->proxy: upload id
  proxy->meta: store upload id in meta for now
  proxy->customer: upload id
end

group upload part flow after switch
  customer->proxy: upload part
  proxy->meta: check if upload id in meta
alt #PeachPuff no upload id in meta - upload created before switch
  meta->proxy: no
  proxy->s3old: upload part
  s3old->proxy: response
else #PaleGreen upload id in meta - upload created after switch
  meta->proxy: yes
  proxy->s3new: upload part
  s3new->proxy: response
end
  proxy->customer: response
end


group finish switch
  worker->queue: drain all old->new tasks
  worker->s3old: poll for dangling multipart upload
  worker->meta: mark SWITCH-DONE
end

== Normal flow but s3new is main now ==


@enduml
```