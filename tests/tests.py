def test_non_existing_query(client):
    assert client.get("/related?query=aliens").status_code == 404


def test_popular_query(client):
    response = client.get("/related?query=hoodie")
    assert response.status_code == 200
    assert len(response.json["related"]) == 3


def test_unpopular_query(client):
    response = client.get("/related?query=ugly")
    assert response.status_code == 200
    assert len(response.json["related"]) == 1


def test_incorrect_input(client):
    assert client.get("/related?query=''").status_code == 400


def test_equal_queries(client):
    response_1 = client.get("/related?query=hoodie")
    response_2 = client.get("/related?query=* hoodie ` ")
    assert response_1.json["related"] == response_2.json["related"]
