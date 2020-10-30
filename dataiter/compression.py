import bz2
import gzip
import io
import lzma
import pathlib
from typing import Generator, IO, Iterable, Union
import zipfile
import zlib


# TODO: Add blosc
COMPRESSION_CLASSES = {
    'bz2': bz2.open,
    'gz': gzip.open,
    'xz': lzma.open,
    None: open,
}
ARCHIVE_CLASSES = {
    'zip': zipfile.ZipFile,
}
FILE_CLASSES = dict(COMPRESSION_CLASSES, **ARCHIVE_CLASSES)
COMPRESSORS = {
    'bz2': bz2.BZ2Compressor,
    'gz': zlib.compressobj,
    'xz': lzma.LZMACompressor,
}
DECOMPRESSORS = {
    'bz2': bz2.BZ2Decompressor,
    'gz': zlib.decompressobj,
    'xz': lzma.LZMADecompressor,
}
DEFAULT_COMPRESSION = 'bz2'


def iter_compress(data_iter: Iterable[bytes], compression: str) -> Generator[bytes]:
    '''
    Compress a data iterable with a compressor.
    '''
    compressor = COMPRESSORS[compression]()

    yield from (compressor.compress(data) for data in data_iter)

    yield compressor.flush()


def iter_decompress(data_iter: Iterable[bytes], compression: str) -> Generator[bytes]:
    '''
    Decompress a data iterable with a decompressor.
    '''
    decompressor = DECOMPRESSORS[compression]()

    return (decompressor.decompress(data) for data in data_iter)


def open_compressed(path: Union[pathlib.Path, str], mode: str='rb') -> IO:
    '''
    Open path for reading which may be compressed or within an archive. Returns a file object and can therefore be used as a
    context manager.

    :type path: pathlib.Path or str
    :param mode: either 'r' or 'rb', mode 'r' will always be in text mode, 'rb' in binary mode.
    :type mode: str
    '''
    if mode not in {'r', 'rb'}:
        raise ValueError(f'unsupported mode: {mode}')
    path = pathlib.Path(path)
    extension = path.suffix.lstrip('.').lower()
    archive_cls = ARCHIVE_CLASSES.get(extension)
    if archive_cls:
        archive = archive_cls(path)  # archive is closed automatically when fileobj within archive is closed
        filenames = archive.namelist()
        if len(filenames) != 1:
            raise IOError('Archives must contain a single file.')

        fileobj = archive.open(filenames[0], 'r')  # opens in binary mode
        if mode == 'r':
            fileobj = io.TextIOWrapper(fileobj)
    else:
        fileobj = COMPRESSION_CLASSES.get(extension, open)(path, 'rt' if mode == 'r' else mode)

    return fileobj


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('input_file_path')
    parser.add_argument('-o', '--output-file-path')
    subparser = parser.add_subparsers(dest='command')
    compress_parser = subparser.add_parser('compress')
    compress_parser.add_argument('compressor')
    decompress_parser = subparser.add_parser('decompress')

    args = parser.parse_args()

    output_path = args.output_file_path

    if args.command == 'compress':
        compressor = COMPRESSION_CLASSES[args.compressor]
        if not output_path:
            output_path = args.input_file_path + f'.{args.compressor}'
        with compressor(output_path, 'w') as output_file, open(args.input_file_path) as input_file:
            output_file.write(input_file.read())
    elif args.command == 'decompress':
        for extension, decompressor in COMPRESSION_CLASSES.items():
            if args.input_file_path.endswith(extension):
                if not output_path:
                    output_path = args.input_file_path[:-(len(extension) + 1)]
                break
        else:
            parser.error('Unknown file extension')
        with open(output_path, 'w') as output_file:
            output_file.write(decompressor(args.input_file_path).read())

