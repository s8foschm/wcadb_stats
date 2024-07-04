import pandas as pd
import time


# DATA IMPORT/EXPORT
def import_data(file_name, filter_key, filter_value):
    with open(file_name) as results_file:
        results = pd.read_csv(results_file, sep="\t")
    results = results.loc[results[filter_key] == filter_value]
    results.drop(
        ['regionalSingleRecord', 'regionalAverageRecord', 'pos', 'best', 'average', 'personCountryId'],
        axis=1, inplace=True)
    results.reset_index(inplace=True, drop=True)
    print("Found and imported results!")
    return results


def import_all_data(file_name):
    with open(file_name) as results_file:
        results = pd.read_csv(results_file, sep="\t")
    results.drop(
        ['regionalSingleRecord', 'regionalAverageRecord', 'pos', 'best', 'average'],
        axis=1, inplace=True)
    # results = results.loc[results['personCountryId'] == 'Germany']
    results.reset_index(inplace=True, drop=True)
    print("Found and imported results!")
    return results


def write_output_file(file_name, data):
    # write results to a tsv file
    with open(file_name, "w+") as file:
        data.to_csv(file, sep="\t")
    # print("Wrote file!")


def create_final_dataframe(sorted_solvetimes, ids_names):
    data = []
    for id in sorted_solvetimes:
        name = ids_names[id]
        time = pretty_print_time(sorted_solvetimes[id])
        data.append({"name": name, "id": id, "time": time})
    dataframe_final = pd.DataFrame(data)
    # print(dataframe_final)
    return dataframe_final


def find_all_german_wca_ids():
    results = import_data("data/WCA_export_Results.tsv", 'personCountryId', 'Germany')
    all_german_ids, all_german_ids_names = collect_ids(results)
    return all_german_ids


# DATA CONVERSION
def replace_dnf_dns(result):
    if result['value1'] == -1 or result['value1'] == -2:
        result['value1'] = 0
    if result['value2'] == -1 or result['value2'] == -2:
        result['value2'] = 0
    if result['value3'] == -1 or result['value3'] == -2:
        result['value3'] = 0
    if result['value4'] == -1 or result['value4'] == -2:
        result['value4'] = 0
    if result['value5'] == -1 or result['value5'] == -2:
        result['value5'] = 0
    return result


def pretty_print_time(time):
    hours, rest1 = divmod(time, 360000)
    minutes, rest2 = divmod(rest1, 6000)
    seconds, centiseconds = divmod(rest2, 100)
    return "{}:{:02d}:{:02d}.{:02d}".format(hours, minutes, seconds, centiseconds)


def fmc_to_time(result):
    attempts = [int(result['value1']), int(result['value2']), int(result['value3'])]
    time = 360000 * (3 - (attempts.count(0) + attempts.count(-1) + attempts.count(
        -2)))  # 1 hour per attempt except DNF or DNS
    return time


def mbf_to_time(result):
    time1, time2, time3 = 0, 0, 0
    if (int(result['value1']) != 0) and (int(result['value1']) != -1) and (int(result['value1']) != -2):
        time1 = int(str(result['value1'])[3:7]) * 100
    if (int(result['value2']) != 0) and (int(result['value2']) != -1) and (int(result['value2']) != -2):
        time2 = int(str(result['value2'])[3:7]) * 100
    if (int(result['value3']) != 0) and (int(result['value3']) != -1) and (int(result['value3']) != -2):
        time3 = int(str(result['value3'])[3:7]) * 100
    time = time1 + time2 + time3
    return time


def mbo_to_time(result):
    time = 0
    if (int(result['value1']) != 0) and (int(result['value1']) != -1) and (int(result['value1']) != -2):
        time = int(str(result['value1'])[5:9]) * 1000
    return time


# CALCULATIONS
def collect_ids(results):
    ids_names = {}
    for index, row in results.iterrows():
        id = row['personId']
        name = row['personName']
        ids_names.update({id: name})
    return list(ids_names.keys()), ids_names


def parse_results(results):
    ids = collect_ids(results)
    results_by_person = {}
    for index, result in results.iterrows():
        id = result['personId']
        if id in results_by_person.keys():
            new_results = results_by_person[id] + [result]
            results_by_person.update({id: new_results})
        else:
            results_by_person.update({id: [result]})
    print("Results parsed!")
    return results_by_person


def add_individual_attempt_times(result):
    event = result['eventId']
    if event == '333fm':
        return fmc_to_time(result)
    elif event == '333mbf':
        return mbf_to_time(result)
    elif event == "333mbo":
        return mbo_to_time(result)
    format = result['formatId']
    result = replace_dnf_dns(result)
    # print(result)
    if format == "a":  # average of 5
        return int(result['value1']) + int(result['value2']) + int(result['value3']) + int(result['value4']) + int(
            result['value5'])
    elif format == "m":  # mean of 3
        return int(result['value1']) + int(result['value2']) + int(result['value3'])
    elif format == "3":  # best of 3
        return int(result['value1']) + int(result['value2']) + int(result['value3'])
    elif format == "2":  # best of 2??? apparently that used to be a thing
        return int(result['value1']) + int(result['value2'])
    elif format == "1":  # best of 1
        return int(result['value1'])
    else:
        print(result)
        raise ValueError(format)


def calculate_personal_solvetimes(ids, results):
    personal_solvetimes = {}
    for id in ids:
        temp_solvetime = 0
        for index, result in results.iterrows():
            if result['personId'] == id:
                individual_time = add_individual_attempt_times(result)
                temp_solvetime += individual_time
                # print(result['eventId'], pretty_print(time(individual_time))
        personal_solvetimes.update({id: temp_solvetime})

    sorted_personal_solvetimes = {key: value for key, value in
                                  sorted(personal_solvetimes.items(), key=lambda item: item[1])}
    return sorted_personal_solvetimes


def calculate_total_scoresheets_per_comp(results):
    return len(results)


def calculate_total_attemps_dnfs_dns(results):
    successes, dnfs, dnss = 0, 0, 0
    for index, result in results.iterrows():
        a1, a2, a3, a4, a5 = int(result['value1']), int(result['value2']), int(result['value3']), int(
            result['value4']), int(result['value5'])
        attempts = [a1, a2, a3, a4, a5]
        curr_no_attempt = attempts.count(0)
        curr_dnfs = attempts.count(-1)
        dnfs += curr_dnfs
        curr_dnss = attempts.count(-2)
        dnss += curr_dnss
        successes += 5 - (curr_no_attempt + curr_dnfs + curr_dnss)
        curr_dnfs = 0
        curr_dnss = 0
    return successes, dnfs, dnss


# ====================


# def personal_sum(person_id):
#    results = import_data("data/WCA_export_Results.tsv", 'personId', person_id)
#    # results = import_data("test_input.tsv", "personId", person_id)
#    # print(results)
#    ids, ids_names = collect_ids(results)
#    name = ids_names[person_id]

#    sorted_personal_solvetimes = calculate_personal_solvetimes([person_id], results)
#    time = sorted_personal_solvetimes[person_id]
#    pretty_time = pretty_print_time(time)

# output_file_name = "person_outputs/output_" + person_id + ".txt"
# write_output_file(output_file_name, results)
#    return name, time, pretty_time


# def rank_total_solvetimes(ids):
#    results = []
#    for index, id in enumerate(ids):
#        print(str(index + 1) + "/" + str(len(ids)) + "\t" + str(id))
#        curr_name, curr_time, curr_pretty_time = personal_sum(id)
#        results.append({"name": curr_name, "id": id, "time": curr_pretty_time, "time_in_cs": curr_time})
#    return results


def personal_sum(results_by_person, person_id):
    results = results_by_person[person_id]
    name = results[0]['personName']
    temp_solvetime = 0
    for result in results:
        individual_time = add_individual_attempt_times(result)
        temp_solvetime += individual_time
        # print(result['eventId'], pretty_print_time(individual_time))
    pretty_time = pretty_print_time(temp_solvetime)
    return name, temp_solvetime, pretty_time


def rank_total_solvetimes(results_by_person, ids):
    results = []
    for index, id in enumerate(ids):
        # print(str(index + 1) + "/" + str(len(ids)) + "\t" + str(id))
        curr_name, curr_time, curr_pretty_time = personal_sum(results_by_person, id)
        results.append({"name": curr_name, "id": id, "time": curr_pretty_time, "time_in_cs": curr_time})
    return results


def german_solvetime_ranking():
    start_time = time.time()
    ids = find_all_german_wca_ids()
    results_import = import_data("data/WCA_export_Results.tsv", "personCountryId", "Germany")
    results_by_person = parse_results(results_import)
    results = rank_total_solvetimes(results_by_person, ids)
    # print(results)

    results_frame = pd.DataFrame(results)
    results_frame.sort_values(by="time_in_cs", inplace=True)
    results_frame.drop(['time_in_cs'], axis=1, inplace=True)

    print(results_frame)

    write_output_file("results/german_solvetime_ranking.tsv", results_frame)
    end_time = time.time()
    execution_time = end_time - start_time
    print("New german ranking execution time: ", pretty_print_time(int(execution_time * 100)))


# def rpo_solvetime_ranking():
#    start_time = time.time()
#    ids = ['2015FEDE01', '2014STEI03', '2019SCHU08', '2016HOLZ01', '2017SCHM09', '2017CATA04', '2023BALL02',
#           '2018SCHU17', '2010KILD02']
#    results = rank_total_solvetimes(ids)

#    results_frame = pd.DataFrame(results)
#    results_frame.sort_values(by="time_in_cs", inplace=True)
#    results_frame.drop(['time_in_cs'], axis=1, inplace=True)

#    print(results_frame)

#    # write_output_file("results/total_solvetimes_rpo_orga.tsv", results_frame)
#    end_time = time.time()
#    execution_time = end_time - start_time
#    print("Old execution time: ", pretty_print_time(int(execution_time * 100)))


def rpo_solvetime_ranking():
    start_time = time.time()
    ids = ['2015FEDE01', '2014STEI03', '2019SCHU08', '2016HOLZ01', '2017SCHM09', '2017CATA04', '2023BALL02',
           '2018SCHU17', '2010KILD02']
    results_import = import_all_data("data/WCA_export_Results.tsv")
    results_by_person = parse_results(results_import)
    results = rank_total_solvetimes(results_by_person, ids)

    results_frame = pd.DataFrame(results)
    results_frame.sort_values(by="time_in_cs", inplace=True)
    results_frame.drop(['time_in_cs'], axis=1, inplace=True)

    print(results_frame)

    # write_output_file("results/total_solvetimes_rpo_orga.tsv", results_frame)
    end_time = time.time()
    execution_time = end_time - start_time
    print("New execution time: ", pretty_print_time(int(execution_time * 100)))


# rpo_solvetime_ranking()
german_solvetime_ranking()

# print("Scoresheets used in total for this comp: ", calculate_total_scoresheets_per_comp(results))
# print("Total attempts: ", calculate_total_attemps_dnfs_dns(results))

# todo
# calculate number of solves per person, DNF-rate per Person, DNF-rate per Event, ...
# calculate total number of scoresheets (number of results)
# total solve time per person over their entire WCA career. NR list of total solve times
