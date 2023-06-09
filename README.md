# bcp

SELECT CHECKSUM_AGG(BINARY_CHECKSUM(TableHash)) AS TableChecksum
FROM (
  SELECT HASHBYTES('SHA2_256', CONCAT(Column1, Column2, Column3, ...)) AS TableHash
  FROM [YourTableName]
  ORDER BY Column1, Column2, Column3, ...
) AS Subquery;


import qrcode

# Create a QR code instance
qr = qrcode.QRCode(
    version=40,  # Specify the version (e.g., Version 40)
    error_correction=qrcode.constants.ERROR_CORRECT_H,  # Set error correction level
    box_size=10,  # Set the size of each QR code module (box)
    border=4  # Set the border size around the QR code
)

# Add data to the QR code
data = "Hello, QR Code!"
qr.add_data(data)

# Generate the QR code
qr.make(fit=True)

# Get the QR code image
qr_image = qr.make_image(fill_color="black", back_color="white")

# Save the QR code image
qr_image.save("qr_code.png")

