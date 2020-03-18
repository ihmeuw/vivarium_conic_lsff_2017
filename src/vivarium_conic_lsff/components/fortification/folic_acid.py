import typing

from vivarium.framework.randomness import get_hash

if typing.TYPE_CHECKING:
    from vivarium.framework.engine import Builder


class FolicAcidFortificationCoverage:

    @property
    def name(self):
        return 'folic_acid_fortification_coverage'

    def setup(self, builder: 'Builder'):
        coverage_data = self.load_coverage_data(builder)

    @staticmethod
    def load_coverage_data(builder: 'Builder'):
        location = builder.configuration.input_data.location
        draw = builder.configuration.input_data.input_draw_number
        seed = get_hash(f'folic_acid_fortification_coverage_draw_{draw}')

        coverage_data = {}
        for coverage_time in ['baseline', 'intervention_start', 'intervention_end']:
            coverage_data[coverage_time] = sum([
                p['weight'] * sample_beta_distribution(seed, p[coverage_time]) for p in COVERAGE[location]
            ])
        return coverage_data


def sample_folic_acid_coverage()





