import click
from pathlib import Path
from typing import Union, List

from loguru import logger
import numpy as np
import pandas as pd
import scipy.stats
from vivarium_public_health.risks.data_transformations import pivot_categorical

from vivarium_conic_lsff import globals as project_globals


def len_longest_location() -> int:
    """Returns the length of the longest location in the project.

    Returns
    -------
       Length of the longest location in the project.
    """
    return len(max(project_globals.LOCATIONS, key=len))


def sanitize_location(location: str):
    """Cleans up location formatting for writing and reading from file names.

    Parameters
    ----------
    location
        The unsanitized location name.

    Returns
    -------
        The sanitized location name (lower-case with white-space and
        special characters removed.

    """
    # FIXME: Should make this a reversible transformation.
    return location.replace(" ", "_").replace("'", "_").lower()


def delete_if_exists(*paths: Union[Path, List[Path]], confirm=False):
    paths = paths[0] if isinstance(paths[0], list) else paths
    existing_paths = [p for p in paths if p.exists()]
    if existing_paths:
        if confirm:
            # Assumes all paths have the same root dir
            root = existing_paths[0].parent
            names = [p.name for p in existing_paths]
            click.confirm(f"Existing files {names} found in directory {root}. Do you want to delete and replace?",
                          abort=True)
        for p in existing_paths:
            logger.info(f'Deleting artifact at {str(p)}.')
            p.unlink()


def read_data_by_draw(artifact_path: str, key : str, draw: int) -> pd.DataFrame:
    """Reads data from the artifact on a per-draw basis. This
    is necessary for Low Birthweight Short Gestation (LBWSG) data.

    Parameters
    ----------
    artifact_path
        The artifact to read from.
    key
        The entity key associated with the data to read.
    draw
        The data to retrieve.

    """
    key = key.replace(".", "/")
    with pd.HDFStore(artifact_path, mode='r') as store:
        index = store.get(f'{key}/index')
        draw = store.get(f'{key}/draw_{draw}')
    draw = draw.rename("value")
    data = pd.concat([index, draw], axis=1)
    data = data.drop(columns='location')
    data = pivot_categorical(data)
    data[project_globals.LBWSG_MISSING_CATEGORY.CAT] = project_globals.LBWSG_MISSING_CATEGORY.EXPOSURE
    return data


class BetaParams:

    def __init__(self, upper_bound, lower_bound, alpha, beta):
        self.upper_bound = upper_bound
        self.lower_bound = lower_bound
        self.support_width = self.upper_bound - self.lower_bound
        self.alpha = alpha
        self.beta = beta

    @classmethod
    def from_statistics(cls, mean, upper_bound, lower_bound, variance=None):
        if variance is None:
            variance = confidence_interval_variance(upper_bound, lower_bound)
        support_width = (upper_bound - lower_bound)
        mean = (mean - lower_bound) / support_width
        variance /= support_width ** 2
        alpha = mean * (mean * (1 - mean) / variance - 1)
        beta = (1 - mean) * (mean * (1 - mean) / variance - 1)
        return cls(upper_bound, lower_bound, alpha, beta)


def sample_beta_distribution(seed: int, params: BetaParams) -> float:
    """Gets a single random draw from a scaled beta distribution.

    Parameters
    ----------
    seed
        Seed for the random number generator.

    Returns
    -------
        The random variate from the scaled beta distribution.

    """
    # Handle degenerate distribution
    if params.upper_bound == params.lower_bound:
        return params.upper_bound

    np.random.seed(seed)
    return params.lower_bound + params.support_width*scipy.stats.beta.rvs(params.alpha, params.beta)


class LogNormParams:

    def __init__(self, sigma, scale):
        self.sigma = sigma
        self.scale = scale

    @classmethod
    def from_statistics(cls, median, upper_bound):
        # 0.975-quantile of standard normal distribution (=1.96, approximately)
        q_975 = scipy.stats.norm().ppf(0.975)
        mu = np.log(median)  # mean of normal distribution for log(variable)
        sigma = (np.log(upper_bound) - mu) / q_975
        return cls(sigma, median)


def sample_lognormal_distribution(seed: int, params: LogNormParams):
    # Handle degenerate distribution
    if params.sigma == 0:
        return params.scale

    np.random.seed(seed)
    return scipy.stats.lognorm.rvs(s=params.sigma, scale=params.scale)


def confidence_interval_variance(upper, lower):
    ninety_five_percent_spread = (upper - lower) / 2
    std_dev = ninety_five_percent_spread / (2 * 1.96)
    return std_dev ** 2
