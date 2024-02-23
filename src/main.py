from .integrity import check_integrity
from .utils import Driver


if __name__ == '__main__':
    
    integrity_pld = check_integrity()

    if integrity_pld is not None:
        driver = Driver(**integrity_pld)
        result = driver.execute()
        
        if result:
            print('='*10, 'execution complete' , '='*10)
