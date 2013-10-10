//
//  MasterViewController.m
//  UbiquityStoreManagerExample
//
//  Created by Aleksey Novicov on 3/27/12.
//  Copyright (c) 2012 Yodel Code LLC. All rights reserved.
//

#import "MasterViewController.h"
#import "DetailViewController.h"
#import "AppDelegate.h"
#import "User.h"
#import "Event.h"

@implementation MasterViewController

- (IBAction)setiCloudState:(id)sender {

    [[AppDelegate sharedAppDelegate].ubiquityStoreManager setCloudEnabled:self.iCloudSwitch.on];
}

- (IBAction)cleariCloud:(id)sender {

    [[AppDelegate sharedAppDelegate].ubiquityStoreManager deleteCloudContainerLocalOnly:NO];
}

- (IBAction)rebuildiCloud:(id)sender {

    [[AppDelegate sharedAppDelegate].ubiquityStoreManager deleteCloudContainerLocalOnly:YES];
}

- (id)initWithNibName:(NSString *)nibNameOrNil bundle:(NSBundle *)nibBundleOrNil {

    if (!(self = [super initWithNibName:nibNameOrNil bundle:nibBundleOrNil]))
        return nil;

    self.title = @"Master";
    if ([[UIDevice currentDevice] userInterfaceIdiom] == UIUserInterfaceIdiomPad) {
        self.clearsSelectionOnViewWillAppear = NO;
        self.contentSizeForViewInPopover = CGSizeMake( 320, 600 );
    }

    return self;
}

- (void)viewDidLoad {

    [super viewDidLoad];

    self.tableView.tableHeaderView = self.tableHeaderView;
    self.navigationItem.leftBarButtonItem = self.editButtonItem;
    self.navigationItem.rightBarButtonItem = [[UIBarButtonItem alloc] initWithBarButtonSystemItem:UIBarButtonSystemItemAdd
                                                                                           target:self action:@selector(insertNewObject:)];

    [self reloadFetchedResults:nil];
    // STEP 3 - Handle USMStoreDidChangeNotification to update the UI.
    [[NSNotificationCenter defaultCenter] addObserver:self selector:@selector(reloadFetchedResults:)
                                                 name:USMStoreDidChangeNotification
                                               object:[AppDelegate sharedAppDelegate].ubiquityStoreManager];
}

- (void)reloadFetchedResults:(NSNotification*)note {

    _fetchedResultsController = nil;
    [self fetchedResultsController];
    [self.tableView reloadData];
}

- (BOOL)shouldAutorotateToInterfaceOrientation:(UIInterfaceOrientation)interfaceOrientation {

    if ([[UIDevice currentDevice] userInterfaceIdiom] == UIUserInterfaceIdiomPhone)
        return (interfaceOrientation != UIInterfaceOrientationPortraitUpsideDown);
    
    return YES;
}

- (void)insertNewObject:(id)sender {

    NSManagedObjectContext *context = self.fetchedResultsController.managedObjectContext;
    User *user = [[[AppDelegate sharedAppDelegate] primaryUser] userInContext:context];

    [context performBlockAndWait:^{
        Event *event = [NSEntityDescription insertNewObjectForEntityForName:NSStringFromClass( [Event class] )
                                                     inManagedObjectContext:context];
        event.timeStamp = [NSDate date].timeIntervalSinceReferenceDate;
        event.user = user;

        // Save the context.
        NSError *error = nil;
        if (![context save:&error])
            NSLog( @"Failed saving new Event: %@\n%@", error, [error userInfo] );
    }];
}

#pragma mark - Table View

- (NSInteger)numberOfSectionsInTableView:(UITableView *)tableView {

    return [[self.fetchedResultsController sections] count];
}

- (NSInteger)tableView:(UITableView *)tableView numberOfRowsInSection:(NSInteger)section {

    id<NSFetchedResultsSectionInfo> sectionInfo = [[self.fetchedResultsController sections] objectAtIndex:section];
    return [sectionInfo numberOfObjects];
}

- (UITableViewCell *)tableView:(UITableView *)tableView cellForRowAtIndexPath:(NSIndexPath *)indexPath {

    static NSString *CellIdentifier = @"Cell";
    UITableViewCell *cell = [tableView dequeueReusableCellWithIdentifier:CellIdentifier];
    
    if (!cell) {
        cell = [[UITableViewCell alloc] initWithStyle:UITableViewCellStyleDefault reuseIdentifier:CellIdentifier];
        if ([[UIDevice currentDevice] userInterfaceIdiom] == UIUserInterfaceIdiomPhone)
            cell.accessoryType = UITableViewCellAccessoryDisclosureIndicator;
    }

    Event *event = [self.fetchedResultsController objectAtIndexPath:indexPath];
    cell.textLabel.text = [NSString stringWithFormat:@"%.2f", event.timeStamp];

    return cell;
}

- (BOOL)tableView:(UITableView *)tableView canEditRowAtIndexPath:(NSIndexPath *)indexPath {

    return YES;
}

- (void)tableView:(UITableView *)tableView commitEditingStyle:(UITableViewCellEditingStyle)editingStyle
forRowAtIndexPath:(NSIndexPath *)indexPath {

    if (editingStyle == UITableViewCellEditingStyleDelete) {
        NSManagedObjectContext *context = self.fetchedResultsController.managedObjectContext;

        [context performBlockAndWait:^{
            [context deleteObject:[self.fetchedResultsController objectAtIndexPath:indexPath]];

            NSError *error = nil;
            if (![context save:&error])
                NSLog( @"Failed saving after delete: %@\n%@", error, [error userInfo] );
        }];
    }
}

- (BOOL)tableView:(UITableView *)tableView canMoveRowAtIndexPath:(NSIndexPath *)indexPath {

    return NO;
}

- (void)tableView:(UITableView *)tableView didSelectRowAtIndexPath:(NSIndexPath *)indexPath {

    [self.tableView deselectRowAtIndexPath:indexPath animated:YES];

    NSManagedObject *object = [[self fetchedResultsController] objectAtIndexPath:indexPath];
    if ([[UIDevice currentDevice] userInterfaceIdiom] == UIUserInterfaceIdiomPhone) {
        if (!self.detailViewController)
            self.detailViewController = [[DetailViewController alloc] initWithNibName:@"DetailViewController_iPhone" bundle:nil];
        self.detailViewController.detailItem = object;
        [self.navigationController pushViewController:self.detailViewController animated:YES];
    }
    else
        self.detailViewController.detailItem = object;

    NSString *cloudContainerPath = [AppDelegate sharedAppDelegate].ubiquityStoreManager.URLForCloudContainer.path;
    self.detailViewController.fileList = [[NSFileManager defaultManager] subpathsAtPath:cloudContainerPath];
    [self.detailViewController.tableView reloadData];
}

#pragma mark - Fetched results controller

- (NSFetchedResultsController *)fetchedResultsController {

    NSManagedObjectContext *context = [AppDelegate sharedAppDelegate].managedObjectContext;
    if (!context)
        return nil;

    if (!_fetchedResultsController) {
        NSFetchRequest *fetchRequest = [[NSFetchRequest alloc] init];
        NSEntityDescription *entity = [NSEntityDescription entityForName:NSStringFromClass( [Event class] ) inManagedObjectContext:context];
        [fetchRequest setEntity:entity];
        [fetchRequest setFetchBatchSize:20];
        [fetchRequest setSortDescriptors:@[ [[NSSortDescriptor alloc] initWithKey:@"timeStamp" ascending:NO] ]];

        // Edit the section name key path and cache name if appropriate.
        // nil for section name key path means "no sections".
        _fetchedResultsController = [[NSFetchedResultsController alloc] initWithFetchRequest:fetchRequest managedObjectContext:context
                                                                          sectionNameKeyPath:nil cacheName:@"Master"];
        _fetchedResultsController.delegate = self;

        [context performBlockAndWait:^{
            NSError *error = nil;
            if (![_fetchedResultsController performFetch:&error])
                NSLog( @"Failed to fetch Event results: %@\n%@", error, [error userInfo] );
        }];
    }

    return _fetchedResultsController;
}

- (void)controllerDidChangeContent:(NSFetchedResultsController *)controller {

    [self.tableView reloadData];
}

@end
