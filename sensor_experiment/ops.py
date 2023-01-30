import random, os, pathlib, json
from dagster import op, asset, job, ScheduleDefinition, schedule, Definitions, sensor, RunRequest



# @asset
# def fibNumbers():
#     return [1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597, 2584, 4181, 6765, 10946, 17711, 28657, 46368, 75025, 121393, 196418, 317811, 514229, 832040, 1346269, 2178309, 3524578, 5702887, 9227465, 14930352, 24157817, 39088169, 63245986, 102334155, 165580141, 267914296, 433494437, 701408733, 1134903170, 1836311903, 2971215073, 4807526976, 7778742049, 12586269025, 20365011074, 32951280099, 53316291173, 86267571272, 139583862445, 225851433717, 365435296162, 591286729879, 956722026041, 1548008755920, 2504730781961, 4052739537881, 6557470319842, 10610209857723, 17167680177565, 27777890035288, 44945570212853, 72723460248141, 117669030460994, 190392490709135, 308061521170129, 498454011879264, 806515533049393, 1304969544928657, 2111485077978050, 3416454622906707, 5527939700884757, 8944394323791464, 14472334024676221, 23416728348467685, 37889062373143906, 61305790721611591, 99194853094755497, 160500643816367088, 259695496911122585, 420196140727489673, 679891637638612258, 1100087778366101931, 1779979416004714189, 2880067194370816120, 4660046610375530309, 7540113804746346429, 12200160415121876738, 19740274219868223167, 31940434634990099905, 51680708854858323072, 83621143489848422977, 135301852344706746049, 218922995834555169026, 354224848179261915075, 573147844013817084101]

### Scheduled work creation

# This operation and job are just to create additional data to see if sensors and partitioning work.
@op(out=None)#Out(is_required=False))
def make_data(context):
    context.log.info("Current dir: " + str(pathlib.Path(os.path.curdir).resolve()))
    chance = random.random()
    context.log.info(f"Chance: {chance}")
    if chance > 0.3:
        quant = random.randint(1, 4)
        context.log.info(f"Creating {quant} files")
        for i in range(quant):
            num = random.randint(1,16777216)            
            context.log.info(f"Creating {num}.json")
            with open(f"./data/{num}.json", 'w') as fp:
                json.dump({"val":num}, fp)

@job
def make_work():
    make_data()

work_gen = ScheduleDefinition(job=make_work, cron_schedule="* * * * *")
# so this didn't work. . . maybe I need to package it in defs?



## Sensor and work consumption
@op(config_schema={"filepath":str, "field":str})
def get_value_from_json(context):
    filepath = context.op_config['filepath']
    field = context.op_config['field']
    with open(filepath, 'r') as fp:
        value = json.load(fp)[field]
    return value

@op
def collatz(n):
    if n % 2 == 0:
        return n//2
    else:
        return 3 * n + 1

@op
def getCollatzSeries(n):
    rtn = [n]
    while n != 1:
        n = collatz(n)
        rtn.append(n)
    return rtn

@op(out=None)
def log_length(context, lst):
    context.log.info(f"List length = {len(lst)}")

@job
def job_calc_collatz_length():
    val = get_value_from_json()
    series = getCollatzSeries(val)
    log_length(series)

@sensor(job=job_calc_collatz_length)
def my_directory_sensor():
    MY_DIRECTORY = "./data"
    for filename in os.listdir(MY_DIRECTORY):
        filepath = os.path.join(MY_DIRECTORY, filename)
        if os.path.isfile(filepath):
            yield RunRequest(
                run_key=filepath,
                run_config={
                    "ops": {"get_value_from_json": {"config": {"filepath": filepath, 'field': 'val'}}}#, "getCollatzSeries":{}, "log_length":{}}
                },
            )



defs = Definitions(schedules=[work_gen], sensors=[my_directory_sensor])
# that worked, btw