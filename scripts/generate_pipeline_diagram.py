from graphviz import Digraph

# Create a new directed graph
dot = Digraph(comment='eCommerce Lakehouse Pipeline')

# Nodes
dot.node('A', 'Raw Data (data_raw)')
dot.node('B', 'Bronze Layer (bronze)')
dot.node('C', 'Silver Layer (silver)')
dot.node('D', 'Gold Layer (gold)')
dot.node('E', 'BI / Analytics / Reports')

# Edges
dot.edge('A', 'B', label='Ingest scripts')
dot.edge('B', 'C', label='Transform scripts')
dot.edge('C', 'D', label='Aggregate / Enrich')
dot.edge('D', 'E', label='Visualize / Analyze')

# Save the diagram
dot.render('docs/pipeline_diagram', format='png', cleanup=True)
print("✅ Pipeline diagram generated: docs/pipeline_diagram.png")

