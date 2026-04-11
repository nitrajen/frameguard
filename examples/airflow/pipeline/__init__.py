import dfguard.pyspark as dfg

dfg.arm()
# dfg.arm(subset=False)  # strict: no extra columns anywhere in the package

# To disable enforcement globally (e.g. in tests or non-prod environments):
# dfg.disarm()
