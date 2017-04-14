def debugPrint(errmsg):
  ## input is a dictionary of values
  debugF = True
  if debugF:
    errmsg = [str(e) for e in errmsg]
    print("@@@ "+" ".join(errmsg))
  return