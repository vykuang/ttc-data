import pytest

def read_protobuf(pb_path):
    with open(pb_path, 'rb') as pb:
        return pb.read()
    
# save a real .pb once
@pytest.fixture
def get_trip_fixture():
    return read_protobuf("tests/fixtures/trip_update.pb")
    
@pytest.fixture
def get_vehicle_fixture():
    return read_protobuf("tests/fixtures/vehicle_position.pb")