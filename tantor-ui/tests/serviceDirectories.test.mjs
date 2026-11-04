import test from 'node:test';
import assert from 'node:assert/strict';
import { matchesServiceConnection } from '../src/utils/serviceDirectories.ts';

for (const serviceType of ['SCHEMA_REGISTRY', 'KAFKA_CONNECT']) {
  test(`${serviceType}: inspecting either instance excludes other hosts and ports`, () => {
    const paths = [
      { serviceType, hostname: 'node229', ipAddress: '192.168.3.229', restPort: 8081, configFile: '/first/config' },
      { serviceType, hostname: 'node229', ipAddress: '192.168.3.229', restPort: 8082, configFile: '/second/config' },
      { serviceType, hostname: 'broker6', ipAddress: '192.168.3.213', restPort: 8081, configFile: '/third/config' },
    ];
    for (const path of paths) {
      assert.deepEqual(paths.filter(row => matchesServiceConnection(row, { host: path.ipAddress, port: path.restPort })), [path]);
    }
  });
}

test('saved endpoint, hostname, and IPv6 aliases match without guessing unknown directories', () => {
  assert.equal(matchesServiceConnection({ restHost: 'registry.example.com', restPort: 8081 }, { host: ' REGISTRY.EXAMPLE.COM. ', port: 8081 }), true);
  assert.equal(matchesServiceConnection({ hostname: 'broker6', restPort: 8083 }, { host: 'broker6', port: 8083 }), true);
  assert.equal(matchesServiceConnection({ ipAddress: '::1', restPort: 8083 }, { host: '[::1]', port: 8083 }), true);
  assert.equal(matchesServiceConnection({ ipAddress: '192.168.3.229' }, { host: '192.168.3.229', port: 8081 }), false);
  assert.equal(matchesServiceConnection({ restPort: 8081 }, { host: '', port: 8081 }), false);
  assert.equal(matchesServiceConnection({ restHost: 'other', restPort: 8081 }, { host: 'missing', port: 8081 }), false);
});
