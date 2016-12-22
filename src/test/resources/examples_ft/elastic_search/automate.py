#!/usr/bin/python
import os
import re

outer_match = re.compile('write_request_outgoing.*\"b\", 1.*')
inner_match = re.compile('write_request_outgoing.*\"b\", 2.*')

for i in range(1,8):

    f1 = open("src/test/resources/examples_ft/elastic_search/elastic_edb.ded", "r")
    f2 = open("src/test/resources/examples_ft/elastic_search/tmp", "w")
    for line in f1:
        res = outer_match.match(line)
        if res:
            f2.write('write_request_outgoing("C", "Data1", "b", 1)@%d;\n'%i)
        else:
            f2.write(line)
    f1.close()
    f2.close()
    fname1 = "src/test/resources/examples_ft/elastic_search/elastic_edb.ded"
    fname2 = "src/test/resources/examples_ft/elastic_search/tmp"
    os.rename(fname2, fname1)

    for j in range(1,8):

        f1 = open("src/test/resources/examples_ft/elastic_search/elastic_edb.ded", "r")
        f2 = open("src/test/resources/examples_ft/elastic_search/tmp", "w")
        for line in f1:
            res = inner_match.match(line)
            if res:
                f2.write('write_request_outgoing("C", "Data2", "b", 2)@%d;\n'%j)
            else:
                f2.write(line)
        f1.close()
        f2.close()
        fname1 = "src/test/resources/examples_ft/elastic_search/elastic_edb.ded"
        fname2 = "src/test/resources/examples_ft/elastic_search/tmp"
        os.rename(fname2, fname1)

        os.system("sbt \"run-main edu.berkeley.cs.boom.molly.SyncFTChecker \
            src/test/resources/examples_ft/elastic_search/elastic_search_with_seq.ded \
            src/test/resources/examples_ft/elastic_search/elastic_assert.ded \
            --EOT 15 \
            --EFF 0 \
            --nodes a,b,c,C,G \
            --crashes 1\" > op")

        res = os.system("grep 'No counterexamples found' op")

        if not res:
            print "Write at time {0}, Write at time {1}, Success".format(i, j)

        else:
            print "Write at time {0}, Write at time {1}, Failure".format(i, j)
