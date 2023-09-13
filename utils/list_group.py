import h5coro
from utils import args, credentials

h5obj = h5coro.H5Coro(args.granule, args.driver, errorChecking=args.checkErrors, verbose=args.verbose, credentials=credentials)
group = h5obj.listGroup(args.group, w_attr=True, w_inspect=True)
for variable, listing in group.items():
    print(f'{variable}:')
    for key, value in listing.items():
        value_str = "<unsupported>"
        if key == '__metadata__':
            value_str = f'{value}'
        elif value != None:
            value_str = f'{value.values}'
        print(f'  {key}: {value_str}')

