import os
import sqlite3
import sys


class SQLProcessor:

    def __init__(self, db_path):
        self.connection = sqlite3.connect(db_path)
        self.connection.enable_load_extension(True)
        self.connection.execute('select load_extension (\'/usr/lib/sqlite3/pcre.so\') ')

    def execute(self, querry):
        return self.connection.execute(querry).fetchall()


def _update_urls(urls, key, value, rep=True):

    if rep:
       value = '/' + value + '/'

    value = '\'' + value + '\''

    if key not in urls:
        s = set()
        s.add(value)
        urls[key] = s
    else:
        s = urls[key]
        s. add(value)
        urls[key] = s


def _print_result(filepath, handlers):

    with open(filepath, 'w') as f:
        for handler_data in handlers:
            f.write('%s,%s \n' % (handler_data[0], handler_data[1]))

def main(args):

        # TODO: what params to pass?

        print args
        db = SQLProcessor(args[1])

        path = args[2]  # path to data files (categories, brands, cms, promotions)

        file_output = args[3]

        data_files = {'Handlers': '{path}/handlers.txt'.format(path=path),
                      'Category': '{path}/categories.txt'.format(path=path),
                      'Brands':   '{path}/brands.txt'.format(path=path),
                      'CMS':      '{path}/cms.txt'.format(path=path),
                      'Compaign':  '{path}/campaign.txt'.format(path=path)}


        # List all possible URL, like:
            # /handler/v2
            # /category/        and /category (no slash)
            # /cms/             and /cms (no slash)
            # /mobapi/category/ and /mobapi/category (no slash)

        urls = {}
        for key, value in data_files.iteritems():
            with open(r'%s' % value, 'r') as f:
                for line in f:
                    content = line.replace('\n', '').strip().replace('\'', '').replace('\\', '')
                    if key == 'Handlers':
                        _update_urls(urls, key, content, False)
                    else:
                        _update_urls(urls, key, content)
                        _update_urls(urls, key + '_no_slash', '/' + content, False)
                        _update_urls(urls, 'Mobapi' + key, 'mobapi/' + content)
                        _update_urls(urls, 'Mobapi' + key + '_no_slash', '/mobapi/' + content, False)

        # concationated srings to use in 'where in' claause
        sqls_in_clause = {}
        for key, data in urls.iteritems():
            sqls_in_clause[key] = ', '.join(data)

        # TOTAL WITH STATUS=200
        querry = "select 'sum', count(*) from log where status_type = 2"
        req_total_200 = int(db.execute(querry)[0][1])

        sql_prefix = "substr(request_path, 1, instr(ltrim(request_path, '/'), '/') + 1)"
        #sql_prefix_mobapi = "substr(request_path, 1, instr(ltrim(request_path, '/mobapi'), '/') + 8)"
        sql_prefix_mobapi = "substr(request_path, 1, instr(substr(request_path, 9), '/') + 8)"

        querries = {}

        total_processed_sum = 0
        processed = []

        querry1 = ''
        for key, value in sqls_in_clause.iteritems():

            # select all exact matches
            if 'no_slash' in key or 'Handlers' in key:

                if 'Handlers' in key:
                    querry = "select request_path, count(*) from log where status_type =2  and  request_path in (%s) and request_path != '/' group by request_path " % (value )
                else:
                    querry = "select '%s', count(*) from log where status_type =2  and  request_path in (%s) and request_path != '/'" % (key, value)

                querry1 = "select request_path from log where status_type =2  and  request_path in (%s) and request_path != '/' " % (value)

            # select all matches, that starts from /category/
            elif key != 'Handlers' and 'Mobapi' not in key:
                querry = "select '%s',  count(*) from log where  status_type =2  and %s != '/' and %s in (%s) and  request_path not in (%s)" % (key, sql_prefix, sql_prefix, value, sqls_in_clause['Handlers'])
                querry1 = "select request_path from log where status_type =2  and  %s != '/' and %s in (%s) and  request_path not in (%s)" % (sql_prefix, sql_prefix, value, sqls_in_clause['Handlers'])

            # select all mathes, that starts from /mobapi/category/
            elif key not in 'no_slash' and 'Mobapi' in key:
                querry = "select '%s', count(*) from log where  request_path like '/mobapi/%%' and status_type =2  and %s != '/' and %s in (%s) and  request_path not in (%s)" % (key, sql_prefix, sql_prefix_mobapi, value, sqls_in_clause['Handlers'])
                querry1 = "select request_path from log where  request_path like '/mobapi/%%' and status_type =2  and %s != '/' and %s in (%s) and  request_path not in (%s)" % (sql_prefix, sql_prefix_mobapi, value, sqls_in_clause['Handlers'])

            result = db.execute(querry)
            for res in result:
                total_processed_sum += int(res[1])
                processed.append(
                        {'key': res[0], 'sum': int(res[1])})

            querries[key] = querry1

        # HOME PAGE
        querry = "select count(*) from log where status_type=2  and  request_path = '/' "
        result = int(db.execute(querry)[0][0])

        total_processed_sum += result
        processed.append({'key': 'home_page', 'sum': result})

        # PRODUCT PAGE
        querry = "select count(*) from log where status_type=2  and request_path like \'%.html\' and request_path not like '/mobapi/mobapi%'"
        result = int(db.execute(querry)[0][0])

        total_processed_sum += result
        processed.append({'key': 'product_page', 'sum': result})

        # MOBAPI PRODUCT PAGE
        querry = "select count(*) from log where status_type=2  and request_path like \'%.html\' and request_path    like '/mobapi/mobapi%'"
        result = int(db.execute(querry)[0][0])

        total_processed_sum += result
        processed.append({'key': 'mobapi_product_page', 'sum': result})

        # STATIC CONTENT
        querry = "select count(request_path) from log " \
                 "where status_type=2  and  request_path REGEXP '\.[a-zA-Z]{0,5}$' " \
                 "and request_path not like '%.html' " \

        result = int(db.execute(querry)[0][0])

        total_processed_sum += result
        sum_static = result
        processed.append({'key': 'static_content', 'sum': result})
        print (total_processed_sum)

        print 'sum, {total}, Total processed with status 200 (with/without static): {total}/{total_no_static}.  {percent}%% - categorized)'.format(
            total=req_total_200 - sum_static,
            total_no_static=req_total_200,
            percent=round(100 * float(total_processed_sum)/float(req_total_200), 2)
        )

        processed_summary = {}
        processed_summary['sum'] = req_total_200 - sum_static
        for line in processed:

            key = line['key'].replace('_no_slash', '')
            if key in processed_summary:
                gen_sum = processed_summary[key] + line['sum']
                processed_summary[key] = gen_sum
            else:
                processed_summary[key] = {}
                processed_summary[key] = line['sum']

        processed_sorted = sorted(processed_summary.iteritems(), key=lambda (k, v): (v, k), reverse=True)

        _print_result(file_output, processed_sorted)

        querry = "select request_path, count(*) from log " \
          "where status_type=2  and  request_path not in ({categories}) " \
                 "and request_path not in ({handlers}) " \
                 "and request_path not in ({brands}) " \
                 "and request_path not in ({mobapi_br}) " \
                 "and request_path not in ({mobapi_cat}) " \
                "and request_path not in ({mobapi_cat_ns}) " \
                "and request_path not in ({mobapi_compain_ns}) " \
                "and request_path not in ({mobapi_compaign}) " \
                "and request_path not in ({mobapi_brands_ns}) " \
                "and request_path not in ({cat_ns}) " \
                "and request_path not in ({compain_ns}) " \
                "and request_path not in ({brands_ns}) " \
                "and request_path not in ({compain}) " \
                "and request_path not in ({cms}) " \
                "and request_path not in ({cms_ns}) " \
                "and request_path not in ({mobapi_cms}) " \
                "and request_path not in ({mobapi_cms_ns}) " \
          "and request_path not REGEXP '\.[a-zA-Z]{{0,5}}$' " \
          "and request_path not like '%.html' " \
          "and request_path != '/' " \
          "group by request_path".format(
                categories=querries['Category'],
                handlers=querries['Handlers'],
                brands=querries['Brands'],
                mobapi_br=querries['MobapiBrands'],
                mobapi_cat=querries['MobapiCategory'],
                mobapi_cat_ns=querries['MobapiCategory_no_slash'],
                mobapi_compain_ns=querries['MobapiCompaign_no_slash'],
                mobapi_compaign=querries['MobapiCompaign'],
                mobapi_brands_ns=querries['MobapiBrands_no_slash'],
                cat_ns=querries['Category_no_slash'],
                compain_ns=querries['Compaign_no_slash'],
                brands_ns=querries['Brands_no_slash'],
                compain=querries['Compaign'],
                cms=querries['CMS'],
                cms_ns=querries['CMS_no_slash'],
                mobapi_cms=querries['MobapiCMS'],
                mobapi_cms_ns=querries['MobapiCMS_no_slash']
        )

        with open(r'{path}/remained_urls.txt'.format(path=path), 'w') as r:
            for line in db.execute(querry):
                r.write(str(line[0]) + ' ' + str(line[1]) + '\n')

if __name__ == "__main__":
        os.chdir(os.path.dirname(os.path.realpath(__file__)))
        main(sys.argv)
