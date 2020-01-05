#!/bin/bash
tmux -2 new -s blockchain_cluster \; \
  send-keys "ssh -p 19122 apl@223.194.70.105" C-m \; \
  split-window -v -p 50 \; \
  send-keys "ssh -p 19222 apl@223.194.70.105" C-m \; \
  split-window -h -p 50 \; \
  send-keys "ssh -p 19322 apl@223.194.70.105" C-m \; \
  split-window -h -p 50 \; \
  send-keys "ssh -p 19422 apl@223.194.70.105" C-m \; \
  split-window -h -p 50 \; \
  send-keys "ssh -p 19522 apl@223.194.70.105" C-m \; \
  split-window -h -p 50 \; \
  send-keys "ssh -p 19622 apl@223.194.70.105" C-m \; \
  split-window -h -p 50 \; \
  send-keys "ssh -p 19722 apl@223.194.70.105" C-m \; \
  split-window -h -p 50 \; \
  send-keys "ssh -p 19822 apl@223.194.70.105" C-m \; \
  select-layout tiled
exit