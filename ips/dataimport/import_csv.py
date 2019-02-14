from ips.dataimport import CSVType
from ips.dataimport.import_non_response import import_non_response
from ips.dataimport.import_shift import import_shift
from ips.dataimport.import_traffic import import_traffic
from ips.dataimport.import_unsampled import import_unsampled
from ips.dataimport.import_survey import import_survey


def import_csv(file_type, run_id, file_name):

    def import_traffic_file():
        import_traffic(file_name, file_type, run_id)

    def import_shift_file():
        import_shift(file_name, file_type, run_id)

    def import_nonresponse_file():
        import_non_response(file_name, file_type, run_id)

    def import_unsampled_file():
        import_unsampled(file_name, file_type, run_id)

    def import_survey_file():
        import_survey(file_name, run_id)

    if file_type == CSVType.Sea:
        return import_traffic_file

    if file_type == CSVType.Air:
        return import_traffic_file

    if file_type == CSVType.Tunnel:
        return import_traffic_file

    if file_type == CSVType.Shift:
        return import_shift_file

    if file_type == CSVType.NonResponse:
        return import_nonresponse_file

    if file_type == CSVType.Unsampled:
        return import_unsampled_file

    if file_type == CSVType.Survey:
        return import_survey_file

    return None
