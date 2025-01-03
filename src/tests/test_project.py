from seeknal.context import context
from seeknal.project import Project
from seeknal.common_artifact import Source, Rule, Common, Dataset
from seeknal.tasks.sparkengine.loaders import Loader
from seeknal.entity import Entity


def test_create_project():
    project = Project(name="test_project", description="test project")
    project.get_or_create()
    print(project.project_id)


def test_update_project():
    project = Project(name="test_project")
    project.get_or_create()
    project.update(description="test project updated")

    project.list()
    project.update(description="test project")
    project.list()


def test_add_source():
    project = Project(name="test_project", description="test project")
    project.get_or_create()

    source = Source(
        "my_user_stay",
        Dataset(hive_table="user_stay", params={"dateCol": "date_id"}),
        description="user stay",
    )
    source.get_or_create()
    Source.list()


def test_add_source_with_entity():
    project = Project(name="test_project", description="test project")
    project.get_or_create()

    entity = Entity(name="subscriber", join_keys=["msisdn"])
    entity.get_or_create()
    source = Source(
        "my_user_stay_two",
        Dataset(hive_table="user_stay", params={"dateCol": "date_id"}),
        description="user stay",
        entity=entity,
    )
    source.get_or_create()
    Source.list()


def test_update_source():
    project = Project(name="test_project", description="test project")
    project.get_or_create()
    Source(
        name="my_user_stay",
        details=Dataset(hive_table="user_stay", params={"dateCol": "date_id"}),
    ).get_or_create().update(
        description="user stay updated",
        details=Dataset(hive_table="user_stay_blah", params={"dateCol": "date_id"}),
    )
    Source.list()
    Source(name="my_user_stay").get_or_create().update(
        description="user stay",
        details=Dataset(hive_table="user_stay", params={"dateCol": "date_id"}),
    )
    Source.list()


def test_delete_source():
    project = Project(name="test_project", description="test project")
    project.get_or_create()

    Source(
        name="my_user_stay",
        details=Dataset(hive_table="user_stay", params={"dateCol": "date_id"}),
    ).get_or_create().delete()
    Source.list()
    Source(
        name="my_user_stay",
        details=Dataset(hive_table="user_stay", params={"dateCol": "date_id"}),
    ).get_or_create()
    Source.list()


def test_add_rule():
    project = Project(name="test_project", description="test project")
    project.get_or_create()

    Rule(name="foo", value="bar", description="foo bar").get_or_create()
    Rule.list()


def test_update_rule():
    project = Project(name="test_project", description="test project")
    project.get_or_create()

    Rule(name="foo", value="bar", description="foo bar").get_or_create().update(
        value=["bar", "foo"], description="bar foo"
    )
    Rule.list()
    Rule(name="foo").get_or_create().update(value="bar", description="foo bar")
    Rule.list()


def test_delete_rule():
    project = Project(name="test_project", description="test project")
    project.get_or_create()

    Rule(name="foo", value="bar", description="foo bar").get_or_create().delete()
    Rule.list()
    Rule(name="foo", value="bar", description="foo bar").get_or_create()
    Rule.list()


def test_get_common():
    project = Project(name="test_project", description="test project")
    project.get_or_create()

    Rule(name="foo", value="bar", description="foo bar").get_or_create()
    Rule(name="blah", value=["foo", "bar"]).get_or_create()
    Source(
        "my_user_stay",
        Dataset(hive_table="user_stay", params={"dateCol": "date_id"}),
        description="user stay",
    ).get_or_create()
    print(Common.as_yaml())
