from astropy.io import fits


def is_fits_compressed(path_in: str):
    with fits.open(path_in) as hdulist:
        for hdu in hdulist:
            if isinstance(hdu, fits.PrimaryHDU):
                continue
            elif not isinstance(hdu, fits.CompImageHDU):
                return False
    return True


def fits_compress(path_in: str, path_out: str):
    with fits.open(path_out, mode="append") as comphdulist:
        with fits.open(path_in, do_not_scale_image_data=True) as hdulist:
            for hdu in hdulist:
                if isinstance(hdu, fits.PrimaryHDU):
                    compressed_hdu = fits.PrimaryHDU(
                        header=hdu.header, data=hdu.data
                    )
                elif isinstance(hdu, fits.CompImageHDU):
                    compressed_hdu = hdu
                else:
                    compressed_hdu = fits.CompImageHDU(
                        header=hdu.header, data=hdu.data
                    )

                comphdulist.append(compressed_hdu)
