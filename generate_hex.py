import secrets

# Generate a random hex string of length 32
hex_string = secrets.token_hex(16)  # 16 bytes -> 32 hex characters

print(hex_string)
