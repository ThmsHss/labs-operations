echo "IN WORKSPACE DESTROY AT $(date)"
export DEBIAN_FRONTEND=noninteractive

# Check if the lab overrides the default logic
if [ -f /voc/private/python/workspace_destroy.py ]; then
    /voc/private/python/workspace_destroy.py
    RC=$?
else
    /voc/scripts/python/workspace_destroy.py
    RC=$?
fi

echo "OUT WORKSPACE DESTROY AT $(date)"
exit $RC
