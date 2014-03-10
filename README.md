# clj-el-migrate

A little ElasticSearch Migration Tool

I'm using it for copying one ElasticSearch Index into another after setting the correct Date mapping for the BirdWatch application.

To create a new index, run

    curl -XPUT localhost:9200/birdwatch_v2    
    curl -XPUT localhost:9200/birdwatch_v2/tweets/_mapping -d @twitter_mappings.json

Then finally start the tool:

    lein run > migrate.log
    tail -f migrate.log

## License

Copyright © 2014 Matthias Nehlsen

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
