from populate.imports import *

def create_nodes_from_df(graph,df, label):
    for row in tqdm(df.itertuples()):
        node = Node(label, **row._asdict())
        graph.create(node)

def evaluate_str(df_column):
    column = df_column.apply(lambda x: ast.literal_eval(x))
    return column

#generate clusters with parent-child relationship
def generate_cluster_skills(graph, column, rlp_name, created_from):
    matcher = NodeMatcher(graph)

    for skills in tqdm(column):
        nodes = matcher.match("Skill", final_english_name=IN(list(skills))).all()


        for i in range(len(nodes)):
            for j in range(i+1, len(nodes)):
                node = nodes[i]
                other_node = nodes[j]
                rlp = Relationship(node, rlp_name, other_node)
                graph.merge(rlp)
                rlp["created_from"] = created_from



            



