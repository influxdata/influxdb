ping_cancelled=false    # Keep track of whether the loop was cancelled, or succeeded
until nc -z 127.0.0.1 8086; do :; done &
trap "kill $!; ping_cancelled=true" SIGINT
wait $!          # Wait for the loop to exit, one way or another
trap - INT       # Remove the trap, now we're done with it
echo "Done pinging, cancelled=$ping_cancelled"
