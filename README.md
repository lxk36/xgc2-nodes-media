# xgc2-nodes-media

Product-neutral media and camera nodes for the XGC orchestration protocol.
Capture drivers and Media Edge remain independent products; this pack only
defines portable composition and validation contracts.

Current node:

- `xgc.media.runtime-roster-assert/v2` consumes the immutable process receipts,
  canonical `camera-source-roster/v1` artifact ref/digest, and public source
  projection for one Media Edge and one or more H.264 camera sources. It proves
  one-to-one source/receipt alignment and unique binding/source/runtime
  identities, then emits a structured browser and ROS projection roster.

The node is pure. Starting and stopping the camera and Media Edge are composed
with `xgc2-nodes-process`; roster bytes, RTP ports, Unix sockets, materialized
paths, executable paths and credentials remain behind the provider boundary.
