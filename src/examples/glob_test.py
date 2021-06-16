import glob

target = glob.glob(r"../data/*.json")

if target:
    print("found target")
    for item in target:
        print(item)
