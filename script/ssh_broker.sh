#!/bin/bash
tmux -2 new -s broker \; \
  send-keys "ssh -p 19822 apl@223.194.70.105" C-m \; \
exit