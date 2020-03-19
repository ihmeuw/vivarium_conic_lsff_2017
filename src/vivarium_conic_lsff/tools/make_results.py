from pathlib import Path
import shutil

from loguru import logger

from vivarium_conic_lsff.results_processing import process_results


from pathlib import Path
import shutil

from loguru import logger

from vivarium_conic_lsff.results_processing import process_results


def build_results(output_file: str):
    output_file = Path(output_file)
    measure_dir = output_file.parent / 'count_data'
    # results_dir = output_file.parent / 'final_data'
    for d in [measure_dir]:
        if d.exists():
            shutil.rmtree(d)
        d.mkdir(exist_ok=True, mode=0o775)

    logger.info(f'Reading in output data from {str(output_file)}.')
    data, keyspace = process_results.read_data(output_file)
    # logger.info(f'Filtering incomplete data from outputs.')
    # rows = len(data)
    # data = process_results.filter_out_incomplete(data, keyspace)
    # new_rows = len(data)
    # logger.info(f'Filtered {rows - new_rows} from data due to incomplete information.  {new_rows} remaining.')
    data = process_results.aggregate_over_seed(data)
    logger.info(f'Computing raw count and proportion data.')
    measure_data = process_results.make_measure_data(data)
    logger.info(f'Writing raw count and proportion data to {str(measure_dir)}')
    measure_data.dump(measure_dir)
    # TODO: maybe use later
    # logger.info(f'Computing final_data.')
    # final_data = process_results.make_final_data(measure_data)
    # logger.info(f'Writing final data to {str(results_dir)}')
    # final_data.dump(results_dir)
    logger.info('**DONE**')
