import assert from 'node:assert/strict';
import { test } from 'node:test';
import { tagSearch } from './tagSearch.ts';

test('tag searches quote readable labels without interpreting search operators', () => {
  assert.equal(tagSearch('needs review'), '@tag:"needs review"');
  assert.equal(tagSearch('a|b:blue'), '@tag:"a|b:blue"');
  assert.equal(tagSearch('-skip'), '@tag:"-skip"');
  assert.equal(tagSearch('*'), '@tag:"*"');
  assert.equal(tagSearch('調査中'), '@tag:"調査中"');
});

test('tag labels unsupported by the quote syntax use literal percent encoding', () => {
  for (const tag of ['say "review"', 'C:\\triage\\', '%20', ' spaced ', 'a\nb']) {
    const query = tagSearch(tag);
    assert.ok(!query.includes('"'));
    assert.equal(decodeURIComponent(query.slice('@tag:'.length)), tag);
  }
});
