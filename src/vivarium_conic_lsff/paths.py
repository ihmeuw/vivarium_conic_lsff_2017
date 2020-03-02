from pathlib import Path

from vivarium_conic_lsff import utilities, globals as project_globals

ARTIFACT_ROOT = Path(f"/share/costeffectiveness/artifacts/{project_globals.PROJECT_NAME}/")
MODEL_SPEC_DIR = (Path(__file__).parent / 'model_specifications').resolve()
RESULTS_ROOT = Path(f'/share/costeffectiveness/results/{project_globals.PROJECT_NAME}/')
LBWSG_DATA_ROOT = Path(f'/share/costeffectiveness/lbwsg_new/data')


def lbwsg_data_path(measure: str, location: str):
    sanitized_location = utilities.sanitize_location(location)
    return LBWSG_DATA_ROOT / measure / f'{sanitized_location}.hdf'
