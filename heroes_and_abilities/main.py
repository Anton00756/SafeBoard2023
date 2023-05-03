from unittest import TestCase, main


def bipartite_match(graph):  # Алгоритм Хопкрофта - Карпа
    matching = {}
    for u in graph:
        for v in graph[u]:
            if v not in matching:
                matching[v] = u
                break

    while True:
        parents = {}
        unmatched = []
        parent = dict([(u, unmatched) for u in graph])
        for v in matching:
            del parent[matching[v]]
        layer = list(parent)
        while layer and not unmatched:
            new_layer = {}
            for u in layer:
                for v in graph[u]:
                    if v not in parents:
                        new_layer.setdefault(v, []).append(u)
            layer = []
            for v in new_layer:
                parents[v] = new_layer[v]
                if v in matching:
                    layer.append(matching[v])
                    parent[matching[v]] = v
                else:
                    unmatched.append(v)

        if not unmatched:
            return matching

        def recurse(vertex):
            if vertex in parents:
                vertex_parents = parents[vertex]
                del parents[vertex]
                for current_parent in vertex_parents:
                    if current_parent in parent:
                        pu = parent[current_parent]
                        del parent[current_parent]
                        if pu is unmatched or recurse(pu):
                            matching[vertex] = current_parent
                            return 1
            return 0

        for v in unmatched:
            recurse(v)


def distribute(heroes, mission):
    abilities_graph = {}
    for (index, hero) in enumerate(heroes):
        for ability in hero[-1]:
            abilities_graph.setdefault(ability, []).append(index)
    try:
        result = bipartite_match({index: abilities_graph[ability] for (index, ability) in enumerate(mission)})
        if len(result) != len(heroes):
            return ()
        return tuple(heroes[value][0] for (_, value) in sorted(result.items(), key=lambda data: data[0]))
    except KeyError:
        return ()


class TestDistributing(TestCase):
    def setUp(self):
        pass

    def test_one(self):
        heroes = (("Илья М.", (1, 2, 3)),)
        mission = (1,)
        self.assertEqual(distribute(heroes, mission), ("Илья М.",))

    def test_two(self):
        heroes = (("Илья М.", (1, 2, 3)), ("Алёша П.", (1,)))
        mission = (1, 2)
        self.assertEqual(distribute(heroes, mission), ("Алёша П.", "Илья М.",))

    def test_three(self):
        heroes = (("Илья М.", (1, 2, 3)), ("Алёша П.", (1,)), ("Добрыня Н.", (2, 3)))
        mission = (1, 1, 2)
        self.assertEqual(distribute(heroes, mission), ("Илья М.", "Алёша П.", "Добрыня Н."))

    def test_four(self):
        heroes = (("Илья М.", (1, 2, 3)), ("Алёша П.", (1,)))
        mission = (1, 5)
        self.assertEqual(distribute(heroes, mission), ())


if __name__ == "__main__":
    main(verbosity=2)
