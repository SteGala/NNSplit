def sort_ip_addresses(ip_addresses):
    # Split the IP addresses into octets and convert them to integers
    ip_integers = [list(map(int, ip.split('.'))) for ip in ip_addresses]

    # Sort the IP addresses based on the octets
    sorted_ip_integers = sorted(ip_integers)

    # Convert the sorted octets back to IP address strings
    sorted_ip_addresses = ['.'.join(map(str, ip)) for ip in sorted_ip_integers]

    return sorted_ip_addresses

def is_ip_greater_than(ip1, ip2):
    octets1 = list(map(int, ip1.split('.')))
    octets2 = list(map(int, ip2.split('.')))

    for i in range(len(octets1)):
        if octets1[i] < octets2[i]:
            return False
        elif octets1[i] > octets2[i]:
            return True

    return False  # IP addresses are equal

ip_list = ['192.168.0.1', '10.0.0.2', '172.16.0.3', '192.168.1.100']
sorted_ips = sort_ip_addresses(ip_list)
print(ip_list)

count = 0
benchmark = "180.0.0.3"
for ip in sorted_ips:
    if is_ip_greater_than(ip, benchmark):
        break
    count = count + 1

print(count)