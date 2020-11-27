dbinstance=db1133
db=mediabackups

## fill in wikis table
# deleted wikis
tail -n +2 deleted.dblist | while read wiki; do echo "INSERT IGNORE INTO wikis (wiki_name, type) VALUES ('$wiki', 3);"; done | mysql.py -h $dbinstance $db
# closed wikis
tail -n +2 closed.dblist | while read wiki; do echo "INSERT IGNORE INTO wikis (wiki_name, type) VALUES ('$wiki', 4);"; done | mysql.py -h $dbinstance $db
# private wikis
tail -n +2 private.dblist | while read wiki; do echo "INSERT IGNORE INTO wikis (wiki_name, type) VALUES ('$wiki', 2);"; done | mysql.py -h $dbinstance $db
# public wikis
tail -n +2 all.dblist | while read wiki; do echo "INSERT IGNORE INTO wikis (wiki_name, type) VALUES ('$wiki', 1);"; done | mysql.py -h $dbinstance $db

## fill in containers table, as obtained from "swift list | grep -- '-local-public\|-local-deleted" command
grep -- '-local-public' swift_containers | while read container; do echo "INSERT IGNORE INTO swift_containers (swift_container_name, wiki, type) VALUES ('${container}', 2, 1);"; done |mysql.py -h $dbinstance $db
grep -- '-local-deleted' swift_containers | while read container; do echo "INSERT IGNORE INTO swift_containers (swift_container_name, wiki, type) VALUES ('${container}', 2, 2);"; done |mysql.py -h $dbinstance $db

# to translate containers into wikis:
python3 -c "import SwiftMedia; import sys; print(SwiftMedia.SwiftMedia({}).container2wiki(sys.argv[1]))" <container_name>
