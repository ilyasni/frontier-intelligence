// Frontier Intelligence — Neo4j Constraints & Indexes
// Run via: cypher-shell -u neo4j -p $NEO4J_PASSWORD < storage/neo4j/constraints.cypher

CREATE CONSTRAINT workspace_id IF NOT EXISTS
FOR (w:Workspace) REQUIRE w.id IS UNIQUE;

CREATE CONSTRAINT concept_name_workspace IF NOT EXISTS
FOR (c:Concept) REQUIRE (c.name, c.workspace_id) IS UNIQUE;

CREATE CONSTRAINT document_id IF NOT EXISTS
FOR (d:Document) REQUIRE d.id IS UNIQUE;

CREATE CONSTRAINT source_id IF NOT EXISTS
FOR (s:Source) REQUIRE s.id IS UNIQUE;

CREATE CONSTRAINT trend_id IF NOT EXISTS
FOR (t:TrendCluster) REQUIRE t.id IS UNIQUE;

// Indexes for performance
CREATE INDEX concept_mentions IF NOT EXISTS FOR (c:Concept) ON (c.mentions);
CREATE INDEX concept_workspace IF NOT EXISTS FOR (c:Concept) ON (c.workspace_id);
CREATE INDEX document_workspace IF NOT EXISTS FOR (d:Document) ON (d.workspace_id);
CREATE INDEX document_published IF NOT EXISTS FOR (d:Document) ON (d.published_at);

// Fulltext index for NER matching
CREATE FULLTEXT INDEX concept_fulltext IF NOT EXISTS
FOR (c:Concept) ON EACH [c.name, c.aliases];
