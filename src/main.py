from .integrity import check_integrity
from .utils import Driver


if __name__ == '__main__':
    
    integrity_pld = check_integrity()

    if integrity_pld is not None:
        print('='*10, 'Starting Sync' , '='*10)
        driver = Driver(**integrity_pld)
        result = driver.execute()
        
        if result:
            print('='*10, 'Sync Complete' , '='*10)
