const elasticsearch = require('./index.js');

elasticsearch({
  method: 'GET',
  path: [''],
})
.then(res => {
  console.log(res);
}, err => {
  console.error(err);
  process.exitCode = 1;
});
