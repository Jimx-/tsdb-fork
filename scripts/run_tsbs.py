import argparse
import os
import subprocess
import sys
import shutil

#DATASETS = ('s1d-i10m-10000', 's1d-i1h-20000', 's10m-i1m-100000',
#            's1d-i1h-100000', 's10h-i1h-1000000')
#DATASETS_N = (100000, 200000, 1000000, 1000000, 10000000)

DATASETS = ('s1d-i1h-20000', 's1d-i1h-100000', 's10h-i1h-1000000')
DATASETS_N = (200000, 1000000, 10000000)
DATASETS_IDX = (2, 4, 5)


def run_benchmark(benchmark_path, dataset_path, full_db):
    result_path = os.path.join(benchmark_path, 'bench-results')
    try:
        os.makedirs(result_path)
    except FileExistsError:
        pass

    retval = run_insert_fresh(benchmark_path, dataset_path, result_path,
                              full_db)
    if retval != 0:
        return retval

    retval = run_query(benchmark_path, dataset_path, result_path, full_db)
    if retval != 0:
        return retval

    retval = run_insert_fresh(benchmark_path, dataset_path, result_path,
                              full_db, True)
    if retval != 0:
        return retval

    retval = run_query(benchmark_path, dataset_path, result_path, full_db,
                       True)
    if retval != 0:
        return retval

    retval = run_insert(benchmark_path, dataset_path, result_path, full_db)
    if retval != 0:
        return retval

    retval = run_mixed(benchmark_path, dataset_path, result_path, full_db)
    if retval != 0:
        return retval

    return 0


def make_data_path(benchmark_path, dataset, full_db, bitmap_only):
    return os.path.join(
        benchmark_path, 'bench',
        dataset + (full_db and '-full' or '') + (bitmap_only and '-bm' or ''))


def make_option(full_db, bitmap_only):
    opt = ''
    if full_db:
        opt += '-f'
    if bitmap_only:
        opt += ' -b'
    return opt


def make_label(full_db, bitmap_only):
    label = full_db and 'fulldb' or 'hybrid'
    return label + (bitmap_only and '-bm' or '')


def run_insert_fresh(benchmark_path,
                     dataset_path,
                     result_path,
                     full_db,
                     bitmap_only=False):
    retval = 0

    for dataset in DATASETS:
        data_path = make_data_path(benchmark_path, dataset, full_db,
                                   bitmap_only)

        try:
            os.makedirs(data_path)
        except FileExistsError:
            pass

        cmd = '{}/tsbs -w insert -r {} -d {} {}'.format(
            benchmark_path, data_path,
            os.path.join(dataset_path, 'prometheus-data-cpu-only-' + dataset),
            make_option(full_db, bitmap_only))

        with open(
                os.path.join(
                    result_path, '{}-{}-fresh-insert.txt'.format(
                        dataset, make_label(full_db, bitmap_only))), 'w') as f:
            print('Benchmarking fresh insert for {} {}...'.format(
                dataset, make_label(full_db, bitmap_only)))
            code = subprocess.call(cmd.split(' '), stdout=f)

            if code != 0:
                print('Non-zero return code for {}: {}'.format(dataset, code))
                retval = code

    return retval


def run_insert(benchmark_path, dataset_path, result_path, full_db):
    retval = 0

    for dataset in DATASETS:
        data_path = make_data_path(benchmark_path, dataset, full_db, False)
        backup_path = data_path + '_backup'
        if not os.path.exists(backup_path):
            shutil.copytree(data_path, backup_path)
        continue

        cmd = '{}/tsbs -w insert -r {} -d {} {}'.format(
            benchmark_path, data_path,
            os.path.join(dataset_path, 'prometheus-data-cpu-only-' + dataset),
            make_option(full_db, False))

        with open(
                os.path.join(
                    result_path,
                    '{}-{}-insert.txt'.format(dataset,
                                              make_label(full_db, False))),
                'w') as f:
            print('Benchmarking insert for {} {}...'.format(
                dataset, make_label(full_db, False)))
            code = subprocess.call(cmd.split(' '), stdout=f)

            if code != 0:
                print('Non-zero return code for {}: {}'.format(dataset, code))
                retval = code

    return retval


def run_query(benchmark_path,
              dataset_path,
              result_path,
              full_db,
              bitmap_only=False):
    retval = 0

    for s, dataset in enumerate(DATASETS, 1):
        data_path = make_data_path(benchmark_path, dataset, full_db,
                                   bitmap_only)

        for q in range(1, 4):
            cmd = '{}/tsbs -w query -r {} -q {} {}'.format(
                benchmark_path, data_path, q,
                make_option(full_db, bitmap_only))

            with open(
                    os.path.join(
                        result_path, 's{}-q{}-{}-query.txt'.format(
                            DATASETS_IDX[s - 1], q,
                            make_label(full_db, bitmap_only))), 'w') as f:
                print('Benchmarking query for s{}-q{} {}...'.format(
                    s, q, make_label(full_db, bitmap_only)))
                code = subprocess.call(cmd.split(' '), stdout=f)

                if code != 0:
                    print('Non-zero return code for s{}-q{}: {}'.format(
                        s, q, code))
                    retval = code

    return retval


def run_mixed(benchmark_path, dataset_path, result_path, full_db):
    retval = 0
    for dataset, N in zip(DATASETS, DATASETS_N):
        data_path = os.path.join(
            benchmark_path, 'bench',
            dataset + '-mixed' + (full_db and '-full' or ''))
        backup_path = make_data_path(benchmark_path, dataset, full_db, True)

        for ratio in (0.0, 0.3, 0.7, 1.0):
            for size in (0.1, 0.4, 0.7, 1.0):
                if os.path.exists(data_path):
                    shutil.rmtree(data_path)
                shutil.copytree(backup_path, data_path)

                cmd = '{}/tsbs -w mixed -r {} -d {} -a 1.5 -s {} -t {} {}'.format(
                    benchmark_path, data_path,
                    os.path.join(dataset_path,
                                 'prometheus-data-cpu-only-' + dataset),
                    int(size * N), ratio, make_option(full_db, True))

                with open(
                        os.path.join(
                            result_path, '{}-{}-mixed-{}-{}.txt'.format(
                                dataset, full_db and 'fulldb' or 'hybrid',
                                ratio, size)), 'w') as f:
                    print(
                        'Benchmarking mixed workload for {} {} size={} ratio={}...'
                        .format(dataset, full_db and 'fulldb' or 'hybrid',
                                size, ratio))
                    code = subprocess.call(cmd.split(' '), stdout=f)

                if code != 0:
                    print('Non-zero return code for {}: {}'.format(
                        dataset, code))
                    retval = code

    return retval


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('--benchmark-path',
                        type=str,
                        help='Path to benchmark data')

    parser.add_argument('--dataset-path', type=str, help='Path to datasets')

    args = parser.parse_args()

    retval = run_benchmark(args.benchmark_path, args.dataset_path, False)
    if retval != 0:
        sys.exit(retval)

    retval = run_benchmark(args.benchmark_path, args.dataset_path, True)
    sys.exit(retval)
