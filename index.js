const { Readable } = require('stream');
const http = require('http');

module.exports = Object.assign(elasticsearch, {
  scroll,
});

const global_url = new URL(
  process.env.ELASTICSEARCH ||
  'http://127.0.0.1:9200'
);

const k_can_retry = Symbol('can_retry');

async function elasticsearch({
  method = 'GET',
  path,
  body,
  options,
  headers,
  on404,
  retry,
}) {

  const req_content = body && (
    [].concat(body)
    .map(it => (typeof it == 'string' ? it : JSON.stringify(it)) + '\n')
    .join('')
  );

  for (;;) {
    try {
      const [res, res_content_buf] = await new Promise((resolve, reject) => {
        http.request({
          method,
          hostname: global_url.hostname,
          port: global_url.port,
          path: (
            '/' + path.map(encodeURIComponent).join('/') +
            new URLSearchParams(options).toString().replace(/.+/, '?$&')
          ),
          headers: {
            'content-type': 'application/json',
            'connection': 'keep-alive',
            ...headers,
          },
        })
        .on('error', err => reject(Object.assign(err, { [k_can_retry]: true })))
        .on('response', res_inner => {
          const res_chunks = [];
          res_inner
          .on('error', err => reject(Object.assign(err, { [k_can_retry]: true })))
          .on('data', chunk => res_chunks.push(chunk))
          .on('end', _ => resolve([res_inner, Buffer.concat(res_chunks)]));
        })
        .end(req_content);
      });

      if (res.headers.warning) {
        console.warn(res.headers.warning);
      }

      if (res.statusCode == 404 && on404 !== undefined) {
        return on404;
      }

      let res_content;
      try {
        res_content = JSON.parse(res_content_buf);
      } catch (_e) {
        // https://github.com/elastic/elasticsearch/issues/33384
      }

      if (res.statusCode >= 300) {
        throw Object.assign(new Error(), {
          name: 'ElasticSearchError',
          response: res_content,
          [k_can_retry]: res.statusCode >= 500,
          message: (
            res_content &&
            res_content.error &&
            res_content.error.reason
          ),
        });
      }

      if (method == 'HEAD') {
        return true;
      }

      return res_content;
    } catch (err) {
      if (!(retry && err[k_can_retry])) {
        throw err;
      }
    }
  }
};

function scroll(es_req) {
  return new ElasticsearchScroll(es_req);
};

function req_is_readonly({ method, path }) {
  return (
    method == 'GET' ||
    (method == 'POST' && path.slice(-1) == '_search') ||
    (method == 'POST' && path.slice(-1) == '_msearch') ||
    (method == 'POST' && path.join('/') == '_search/scroll')
  );
}

class ElasticsearchScroll extends Readable {
  constructor(request) {
    super({ objectMode: true });
    this.next_request = request;
    this.next_request.options = {
      scroll: '10m',
      ...this.next_request.options,
    };
  }
  _read() {
    elasticsearch(this.next_request).then(({ _scroll_id, hits }) => {
      this.next_request = {
        method: 'POST',
        path: ['_search', 'scroll'],
        body: {
          scroll_id: _scroll_id,
          scroll: '10m',
        },
      };
      const hits_arr = hits.hits;
      if (!hits_arr.length) {
        return this.push(null);
      }
      this.push(hits_arr);
    }, err => {
      this.emit('error', err);
    });
  }
}
