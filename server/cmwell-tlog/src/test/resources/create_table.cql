USE Data;
CREATE TABLE TLog (KEY text PRIMARY KEY) WITH comparator=timestamp AND compaction_strategy_class='LeveledCompactionStrategy';
