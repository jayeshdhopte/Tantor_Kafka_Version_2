package io.translab.tantor.server.config;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.net.spi.InetAddressResolver;
import java.net.spi.InetAddressResolverProvider;
import java.util.stream.Stream;

/**
 * Preserves normal forward DNS resolution while making reverse lookups return
 * the numeric address immediately. This prevents Kafka SASL clients from
 * blocking on unavailable PTR records in air-gapped environments.
 */
public final class FastReverseDnsProvider extends InetAddressResolverProvider {
    @Override
    public InetAddressResolver get(Configuration configuration) {
        return new FastReverseDnsResolver(configuration.builtinResolver());
    }

    @Override
    public String name() {
        return "tantor-fast-reverse-dns";
    }

    private static final class FastReverseDnsResolver implements InetAddressResolver {
        private final InetAddressResolver builtinResolver;

        private FastReverseDnsResolver(InetAddressResolver builtinResolver) {
            this.builtinResolver = builtinResolver;
        }

        @Override
        public Stream<InetAddress> lookupByName(String host, LookupPolicy lookupPolicy)
                throws UnknownHostException {
            return builtinResolver.lookupByName(host, lookupPolicy);
        }

        @Override
        public String lookupByAddress(byte[] address) throws UnknownHostException {
            return InetAddress.getByAddress(address).getHostAddress();
        }
    }
}
