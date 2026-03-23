echo "IN WORKSPACE INIT AT $(date)"
export DEBIAN_FRONTEND=noninteractive

# Check if the lab overrides the default logic
if [ -f /voc/private/python/workspace_init.py ]; then
    /voc/private/python/workspace_init.py
    RC=$?
else
    /voc/scripts/python/workspace_init.py
    RC=$?
fi

echo "OUT WORKSPACE INIT AT $(date)"
exit $RC
