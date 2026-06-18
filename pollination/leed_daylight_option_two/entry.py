from pollination_dsl.dag import Inputs, DAG, task, Outputs
from dataclasses import dataclass
from pollination.honeybee_radiance.post_process import LeedIlluminanceCredits

# input/output alias
from pollination.alias.inputs.model import hbjson_model_input
from pollination.alias.inputs.wea import wea_input
from pollination.alias.inputs.north import north_input
from pollination.alias.inputs.grid import grid_filter_input, \
    min_sensor_count_input, cpu_count
from pollination.alias.inputs.bool_options import glare_control_devices_input
from pollination.alias.inputs.radiancepar import rad_par_leed_illuminance_input
from pollination.alias.outputs.daylight import illuminance_9am_results, \
    illuminance_3pm_results, pass_fail_9am_results, pass_fail_3pm_results, \
    pass_fail_comb_results, leed_two_credit_summary, leed_two_summary_grid

from .point_in_time._illuminance import PointInTimeGridEntryPoint
from ._prepare_folder import LeedDaylightOptionTwoPrepareFolder
from ._visualization import LeedDaylightOptionTwoVisualization


@dataclass
class LeedDaylightOptionTwoEntryPoint(DAG):
    """LEED Daylight Illuminance entry point."""

    # inputs
    model = Inputs.file(
        description='A Honeybee model in HBJSON file format.',
        extensions=['json', 'hbjson'],
        alias=hbjson_model_input
    )

    wea = Inputs.file(
        description='A Typical Meteorological Year (TMY) .wea file. The file '
        'must be annual with a timestep of 1 for a non-leap year.',
        extensions=['wea', 'epw'], alias=wea_input
    )

    glare_control_devices = Inputs.str(
        description='A switch to note whether the model has "view-preserving automatic '
        '(with manual override) glare-control devices," which means that illuminance '
        'only needs to be above 300 lux and not between 300 and 3000 lux.',
        default='glare-control',
        spec={'type': 'string', 'enum': ['glare-control', 'no-glare-control']},
        alias=glare_control_devices_input
    )

    north = Inputs.float(
        default=0,
        description='A number for rotation from north.',
        spec={'type': 'number', 'minimum': 0, 'maximum': 360},
        alias=north_input
    )

    grid_filter = Inputs.str(
        description='Text for a grid identifier or a pattern to filter the sensor grids '
        'of the model that are simulated. For instance, first_floor_* will simulate '
        'only the sensor grids that have an identifier that starts with '
        'first_floor_. By default, all grids in the model will be simulated.',
        default='*',
        alias=grid_filter_input
    )

    cpu_count = Inputs.int(
        default=50,
        description='The maximum number of CPUs for parallel execution. This will be '
        'used to determine the number of sensors run by each worker.',
        spec={'type': 'integer', 'minimum': 1},
        alias=cpu_count
    )

    min_sensor_count = Inputs.int(
        description='The minimum number of sensors in each sensor grid after '
        'redistributing the sensors based on cpu_count. This value takes '
        'precedence over the cpu_count and can be used to ensure that '
        'the parallelization does not result in generating unnecessarily small '
        'sensor grids. The default value is set to 500.', default=500,
        spec={'type': 'integer', 'minimum': 1},
        alias=min_sensor_count_input
    )

    radiance_parameters = Inputs.str(
        description='The radiance parameters for ray tracing',
        default='-ab 5 -aa 0.1 -ad 2048 -ar 64',
        alias=rad_par_leed_illuminance_input
    )

    @task(template=LeedDaylightOptionTwoPrepareFolder)
    def prepare_folder(
        self, model=model, wea=wea, grid_filter=grid_filter, north=north
        ):
        return [
            {
                'from': LeedDaylightOptionTwoPrepareFolder()._outputs.sky_list
            },
            {
                'from': LeedDaylightOptionTwoPrepareFolder()._outputs.model_folder,
                'to': 'model'
            },
            {
                'from': LeedDaylightOptionTwoPrepareFolder()._outputs.resources,
                'to': 'resources'
            },
            {
                'from': LeedDaylightOptionTwoPrepareFolder()._outputs.simulation,
                'to': 'simulation'
            }
        ]

    @task(
        template=PointInTimeGridEntryPoint,
        needs=[prepare_folder],
        loop=prepare_folder._outputs.sky_list,
        sub_folder='simulation/{{item.id}}',
        sub_paths={
            'sky': 'skies/{{item.path}}',
            'sensor_grids_file': 'grids_info.json',
            'bsdfs': 'bsdf'
        }
    )
    def illuminance_simulation(
        self,
        model_folder=prepare_folder._outputs.model_folder,
        sky=prepare_folder._outputs.resources,
        sensor_grids_file=prepare_folder._outputs.resources,
        grid_filter=grid_filter,
        cpu_count=cpu_count,
        min_sensor_count=min_sensor_count,
        radiance_parameters=radiance_parameters,
        bsdfs=prepare_folder._outputs.model_folder
    ):
        # this task doesn't return a folder for each loop.
        # instead we access the results folder as a separate task
        pass

    @task(
        template=LeedIlluminanceCredits,
        needs=[prepare_folder, illuminance_simulation]
    )
    def evaluate_credits(
        self, folder=prepare_folder._outputs.simulation, glare_control_devices=glare_control_devices
    ):
        return [
            {
                'from': LeedIlluminanceCredits()._outputs.pass_fail_results,
                'to': 'results'
            },
            {
                'from': LeedIlluminanceCredits()._outputs.credit_summary,
                'to': 'credit_summary.json'
            }
        ]

    @task(
        template=LeedDaylightOptionTwoVisualization,
        needs=[prepare_folder, illuminance_simulation, evaluate_credits]
    )
    def create_visualization(
        self, model=model, illuminance_9am='simulation/9AM/results',
        illuminance_3pm='simulation/3PM/results', pass_fail_9am='results/9AM',
        pass_fail_3pm='results/3PM', pass_fail_combined='results/combined'
    ):
        return [
            {
                'from': LeedDaylightOptionTwoVisualization()._outputs.visualization,
                'to': 'visualization.vsf'
            }
        ]

    visualization = Outputs.file(
        source='visualization.vsf',
        description='Visualization in VisualizationSet format.'
    )

    illuminance_9am = Outputs.folder(
        source='simulation/9AM/results',
        description='Illuminance results for the 9AM simulation in lux.',
        alias=illuminance_9am_results
    )

    illuminance_3pm = Outputs.folder(
        source='simulation/3PM/results',
        description='Illuminance results for the 3PM simulation in lux.',
        alias=illuminance_3pm_results
    )

    pass_fail_9am = Outputs.folder(
        description='Pass/Fail results for the 9AM simulation as one/zero values.',
        source='results/9AM',
        alias=pass_fail_9am_results
    )

    pass_fail_3pm = Outputs.folder(
        description='Pass/Fail results for the 3PM simulation as one/zero values.',
        source='results/3PM',
        alias=pass_fail_3pm_results
    )

    pass_fail_combined = Outputs.folder(
        description='Pass/Fail results for the combined simulation as one/zero values.',
        source='results/combined',
        alias=pass_fail_comb_results
    )

    space_summary = Outputs.file(
        description='CSV file containing the percentage of the sensor grid area in '
        'each space that meets the criteria.',
        source='results/space_summary.csv',
        alias=leed_two_summary_grid
    )

    credit_summary = Outputs.file(
        description='JSON file containing the number of LEED credits achieved and '
        'a summary of the percentage of the sensor grid area that meets the criteria.',
        source='credit_summary.json',
        alias=leed_two_credit_summary
    )
