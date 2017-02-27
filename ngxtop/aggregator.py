import os
import sqlite3
import sys



def main(args):

        print args

        path = args[1]  # path to data files (categories, brands, cms, promotions)

        file_output = args[2]
        time_prefix = 'test'

        data_files = {'Alice': '{path}/alice.stats.{time_prefix}'.format(path=path, time_prefix=time_prefix),
                      'Mobile': '{path}/mobile.stats.{time_prefix}'.format(path=path, time_prefix=time_prefix)}
                      #'Suggest': '{path}/suggest.stats.{time_prefix}'.format(path=path, time_prefix=time_prefix)}

        nodes = {'Alice': 20,
                 'Mobile': 6,
                 #'Suggest': 8
        }

        handlers = {}
        proportion = {}
        request_count = 0

        for key, path in data_files.iteritems():
            with open(path, 'r') as f:
                request_count += (float(f.readline().split(',')[1]) * float(nodes[key]))

        for key, path in data_files.iteritems():
            with open(path, 'r') as f:
                proportion[key] = (float(f.readline().split(',')[1]) * float(nodes[key]))/float(request_count)

        for key, path in data_files.iteritems():
            with open(path, 'r') as f:
                count = float(f.readline().split(',')[1])
                for line in f:
                    data = line.split(',')
                    handlers[data[0]] = round((float(data[1]) / float(count) * 100) * float(proportion[key]), 3)

        with open(file_output, 'w') as f:
            for handler, count in sorted(handlers.iteritems(), key=lambda (k, v): (v, k), reverse=True):
                print ('%s, %s' % (handler, count))
                f.write('%s,%s\n' % (handler, count))


if __name__ == "__main__":
        os.chdir(os.path.dirname(os.path.realpath(__file__)))
        main(sys.argv)
