import argparse
import os
import subprocess
import sys

DATASETS = ('s1d-i10m-10000', 's1d-i1h-20000', 's10m-i1m-100000',
            's1d-i1h-100000', 's10h-i1h-1000000')
DATASETS_N = (100000, 200000, 1000000, 1000000, 10000000)


def run_benchmark(benchmark_path, dataset_path):
    result_path = os.path.join(benchmark_path, 'bench-results')
    try:
        os.makedirs(result_path)
    except FileExistsError:
        pass

    retval = run_insert_fresh(benchmark_path, dataset_path, result_path)
    if retval != 0:
        return retval

    retval = run_query(benchmark_path, dataset_path, result_path)
    if retval != 0:
        return retval

    retval = run_insert_fresh(benchmark_path, dataset_path, result_path, True)
    if retval != 0:
        return retval

    retval = run_query(benchmark_path, dataset_path, result_path, True)
    if retval != 0:
        return retval

    retval = run_insert(benchmark_path, dataset_path, result_path)
    if retval != 0:
        return retval

    retval = run_mixed(benchmark_path, dataset_path, result_path)
    if retval != 0:
        return retval

    return 0


def run_insert_fresh(benchmark_path,
                     dataset_path,
                     result_path,
                     bitmap_only=False):
    retval = 0

    for dataset in DATASETS:
        data_path = os.path.join(benchmark_path, 'bench',
                                 dataset + (bitmap_only and '-bm' or ''))

        try:
            os.makedirs(data_path)
        except FileExistsError:
            pass

        cmd = '{}/tsbs -w insert -r {} -d {} {}'.format(
            benchmark_path, data_path,
            os.path.join(dataset_path, 'prometheus-data-cpu-only-' + dataset),
            bitmap_only and '-b' or '')

        with open(
                os.path.join(
                    result_path, '{}-hybrid{}-fresh-insert.txt'.format(
                        dataset, bitmap_only and '-bm' or '')), 'w') as f:
            print('Benchmarking fresh insert for {} {}...'.format(
                dataset, bitmap_only and 'bitmap-only' or ''))
            code = subprocess.call(cmd.split(' '), stdout=f)

            if code != 0:
                print('Non-zero return code for {}: {}'.format(dataset, code))
                retval = code

    return retval


def run_insert(benchmark_path, dataset_path, result_path):
    retval = 0

    for dataset in DATASETS:
        data_path = os.path.join(benchmark_path, 'bench', dataset)

        cmd = '{}/tsbs -w insert -r {} -d {}'.format(
            benchmark_path, data_path,
            os.path.join(dataset_path, 'prometheus-data-cpu-only-' + dataset))

        with open(
                os.path.join(result_path,
                             '{}-hybrid-insert.txt'.format(dataset)),
                'w') as f:
            print('Benchmarking insert for {}...'.format(dataset))
            code = subprocess.call(cmd.split(' '), stdout=f)

            if code != 0:
                print('Non-zero return code for {}: {}'.format(dataset, code))
                retval = code

    return retval


def run_query(benchmark_path, dataset_path, result_path, bitmap_only=False):
    retval = 0

    for s, dataset in enumerate(DATASETS, 1):
        data_path = os.path.join(benchmark_path, 'bench',
                                 dataset + (bitmap_only and '-bm' or ''))

        for q in range(1, 4):
            cmd = '{}/tsbs -w query -r {} -q {} {}'.format(
                benchmark_path, data_path, q, bitmap_only and '-b' or '')

            with open(
                    os.path.join(
                        result_path, 's{}-q{}-hybrid{}-query.txt'.format(
                            s, q, bitmap_only and '-bm' or '')), 'w') as f:
                print('Benchmarking query for s{}-q{} {}...'.format(
                    s, q, bitmap_only and 'bitmap-only' or ''))
                code = subprocess.call(cmd.split(' '), stdout=f)

                if code != 0:
                    print('Non-zero return code for s{}-q{}: {}'.format(
                        s, q, code))
                    retval = code

    return retval


def run_mixed(benchmark_path, dataset_path, result_path):
    retval = 0
    for dataset, N in zip(DATASETS, DATASETS_N):
        data_path = os.path.join(benchmark_path, 'bench', dataset + '-mixed')
        try:
            os.makedirs(data_path)
        except FileExistsError:
            pass

        for ratio in (0.0, 0.3, 0.7, 1.0):
            for size in (0.1, 0.4, 0.7, 1.0):
                cmd = '{}/tsbs -w mixed -r {} -d {} -a 1.5 -s {} -t {}'.format(
                    benchmark_path, data_path,
                    os.path.join(dataset_path,
                                 'prometheus-data-cpu-only-' + dataset),
                    int(size * N), ratio)

                with open(
                        os.path.join(
                            result_path, '{}-hybrid-mixed-{}-{}.txt'.format(
                                dataset, ratio, size)), 'w') as f:
                    print(
                        'Benchmarking mixed workload for {} size={} ratio={}...'
                        .format(dataset, size, ratio))
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

    retval = run_benchmark(args.benchmark_path, args.dataset_path)

    sys.exit(retval)
