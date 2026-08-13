# xgc2-nodes-media

Product-neutral media and camera nodes for the XGC orchestration protocol.
Capture drivers and Media Edge remain independent products; this pack only
defines portable composition and validation contracts.

Current node:

- `xgc.media.runtime-roster-assert/v1` consumes the immutable process receipts
  for one Media Edge and one or more H.264 camera sources. It proves one-to-one
  source/receipt alignment, unique binding/source/runtime identities, unique
  loopback RTP ports and control sockets, then emits a structured browser and
  ROS projection roster.

The node is pure. Starting and stopping the camera and Media Edge are composed
with `xgc2-nodes-process`; their executable paths, credentials and host details
remain behind the provider boundary.
