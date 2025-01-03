from seeknal.entity import Entity


def test_create_entity():
    entity = Entity(name="subscriber", join_keys=["msisdn"])
    entity.get_or_create()
    print(entity.entity_id)
    Entity.list()


def test_update_entity():
    entity = Entity(name="subscriber", join_keys=["msisdn"])
    entity.get_or_create()
    entity.update(description="subscriber entity")
    Entity.list()
    entity.update(description="subscriber")
    Entity.list()
